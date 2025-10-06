

from postbound.optimizer.strategies.dynprog import RelOptInfo
from postbound._qep import QueryPlan
from enum import Enum
from postbound.optimizer.strategies.dynprog import *
import postbound.db._db 


class PathKeyComparison(Enum):
    PATHKEYS_EQUAL = "equal"       # equal sorting
    PATHKEYS_BETTER1 = "better1"   # sorting1 ⊇ sorting2
    PATHKEYS_BETTER2 = "better2"   # sorting2 ⊇ sorting1
    PATHKEYS_DIFFERENT = "different" #different sorting

class PathCostComparison(Enum):
    COSTS_EQUAL = "equal"       # path costs fuzzily equal
    COSTS_BETTER1 = "better1"   # first set dominates
    COSTS_BETTER2 = "better2"   # second set dominates
    COSTS_DIFFERENT = "different" # no set dominates 

class SubsetComparison(Enum):
    SETS_EQUAL = "equal"    # table sets are equal
    SETS_SUBSET1 = "subset1" # first table is subset of second
    SETS_SUBSET2 = "subset2" # second table is subset of first
    SETS_DIFFERENT = "different" # neither is subset of the other

STD_FUZZ_FACTOR = 1.01


def postgres_add_path_hook_standard(enumerator: PostgresDynProg,rel: RelOptInfo, path: QueryPlan) -> None: 
    """
    Standard hook for to decide if a path should be added to the pathlist of a relation. 
    
    Without any additional aggressive pruning beforehand. This function mimics PostgreSQLs add_path function.
    """
    add_path_variation(enumerator, rel, path)

def postgres_add_path_hook_1(enumerator: PostgresDynProg, rel: RelOptInfo, path: QueryPlan) -> None:
    """
    A hook using the same add_path_variation as the standard hook. 
    
    Introducing a more aggressive pruning strategie for join-relations in the candidate plan.
    """
    query = enumerator.query
    if plan_traversal(enumerator, query, path):
        add_path_variation(enumerator, rel, path)
    else: pass


def plan_traversal(enumerator: PostgresDynProg, query: SqlQuery, plan: QueryPlan) -> bool:
    """
    A simple traversal function for the query plan. Disregarding leaf nodes for now, as well as intermediates.
    Specifically looks at nodes with join operators and their respective child nodes.

    """
    # empty leaves
    if plan is None:
        return True
    # skip leaves
    if plan.node_type in [ScanOperator.BitmapScan, ScanOperator.IndexScan, ScanOperator.SequentialScan, 
                          ScanOperator.IndexOnlyScan] or plan.node_type in ["Seq. Scan", "Idx. Scan", "Idx-only Scan", 
                                                                            "Bitmap Scan"]:
        return True
    # skip intermediate operators
    if plan.node_type in [IntermediateOperator.Sort, IntermediateOperator.Materialize, 
                          IntermediateOperator.Memoize] or plan.node_type in ["Sort", "Materialize", "Memoize"]:
        return True
    # process non-leaf nodes
    ok = distinguish_join_types(enumerator, query, plan)

    left_ok = plan_traversal(enumerator, query, plan.inner_child)
    right_ok = plan_traversal(enumerator, query, plan.outer_child)

    return ok and left_ok and right_ok
    

def distinguish_join_types(enumerator: PostgresDynProg, query: SqlQuery, plan: QueryPlan) -> bool:
    """
    Categorizes the join type of a plan and calls the appropriate check function.
    If the plan is not a join operator, it raises an error.

    """
    if plan.node_type == JoinOperator.NestedLoopJoin:
        return nested_loop_join_check(enumerator,query, plan)
    elif plan.node_type == JoinOperator.HashJoin:
        return True
    elif plan.node_type == JoinOperator.SortMergeJoin:
        return merge_join_check(enumerator, query, plan)
    elif not plan.is_join():
        return True
    else:
        return True


def nested_loop_join_check(enumerator: PostgresDynProg, query: SqlQuery, plan: QueryPlan) -> bool:
    """
    Checks if the nested loop join is applicable based on the outer and inner child of the plan.
    If the inner relation has an index on the join key, the nested loop join is a valid suitor and 
    may be compared within the pathlist.

    """
    if plan.node_type is not JoinOperator.NestedLoopJoin:
        return False
    inner_child = plan.inner_child
    if inner_child.is_scan() and inner_child.node_type in [ScanOperator.IndexScan, 
                                                           ScanOperator.IndexOnlyScan] or inner_child.node_type in ["Idx. Scan", 
                                                                                                                    "Idx-only Scan"]:
        
        return True
    # at this point we could check if the base table is scanned by and IndexScan and work our way up, checking if a
    # HashJoin, Sort or previous NLJ would "destroy" our index, if not it might still be viable
    return False
       


def merge_join_check(enumerator: PostgresDynProg, query: SqlQuery, plan: QueryPlan) -> bool:
    """
    Checks if the merge join is applicable based on the outer and inner child of the plan.
    If both, the outer and the inner relation are sorted on the join key, the merge join is a valid suitor and
    may be compared within the pathlist.
    Also if there is a GROUPBY or ORDERBY clause in our original query, the merge join is kept and compared, 
    since the ordering might be useful later on.

    """ 
    if plan.node_type is not JoinOperator.SortMergeJoin or plan.node_type != "Sort-Merge Join":
        return False
    
    order_by_ok = query.orderby_clause is not None
    group_by_ok = query.groupby_clause is not None
    if order_by_ok or group_by_ok:
        return True
    
    if plan.outer_child.node_type is IntermediateOperator.Sort or plan.outer_child.node_type == "Sort":
        return False
    else: 
        pass
    if plan.inner_child.node_type is IntermediateOperator.Sort or plan.inner_child.node_type == "Sort":
        return False
    else: 
        pass
    # no sort needed, input relations are likely sorted which the enumerator figured out for us
    return True



def add_path_variation(enumerator: PostgresDynProg, rel: RelOptInfo, path: QueryPlan) -> None:
    """
    An implementation of the add_path function from PostgreSQL, which evaluates a new candidate plan on 
    its cost, cardinality and sort keys. If the new plan is better than an already existing plans in the pathlist, it is added
    to the respective position. If the new plan is worse than an existing plan, it is discarded.
    If the new plan is equal to an existing plan, it is discarded as well.
    """
    accept_new = True # unless old_path is superior
    insert_at = 0 
    new_rows = path.estimated_cardinality

    for i, old_path in enumerate(rel.pathlist):
        remove_old = False
        old_rows = old_path.estimated_cardinality

        #do a fuzzy cost comparison
        costcmp = compare_path_costs_fuzzily(path, old_path, STD_FUZZ_FACTOR)

        if costcmp != PathCostComparison.COSTS_DIFFERENT:

            keyscmp = compare_pathkeys(enumerator, new_keys = path.sort_keys, old_keys = old_path.sort_keys)
            if keyscmp != PathKeyComparison.PATHKEYS_DIFFERENT:
                match costcmp:

                    case PathCostComparison.COSTS_EQUAL:
                        outercmp = subset_compare(path, old_path)
                        if keyscmp == PathKeyComparison.PATHKEYS_BETTER1:
                            if ((outercmp == SubsetComparison.SETS_EQUAL or 
                                outercmp == SubsetComparison.SETS_SUBSET1) and 
                                new_rows <= old_rows):
                                remove_old = True   # new dominates old
                        elif keyscmp == PathKeyComparison.PATHKEYS_BETTER2:
                            if ((outercmp == SubsetComparison.SETS_EQUAL or 
                                outercmp == SubsetComparison.SETS_SUBSET2) and 
                                (new_rows >= old_rows)):
                                accept_new = False  # old dominates new
                        else: # keyscmp == PATHKEYS_EQUAL
                            if outercmp == SubsetComparison.SETS_EQUAL:
                                if new_rows < old_rows:
                                    remove_old = True   # new dominates old
                                elif new_rows > old_rows:
                                    accept_new = False  # old dominates new
                                elif compare_path_costs_fuzzily(path, old_path, STD_FUZZ_FACTOR) == PathCostComparison.COSTS_BETTER1:
                                    remove_old = True   # new dominates old
                                else: accept_new = False # old equals or dominates new
                            elif ((outercmp == SubsetComparison.SETS_SUBSET1) and 
                                  (new_rows <= old_rows)):
                                remove_old = True   # new dominates old
                            elif ((outercmp == SubsetComparison.SETS_SUBSET2) and
                                  (new_rows >= old_rows)):
                                accept_new = False  # old dominates new
                                #else different parameterization, keep both
                        break
                    case PathCostComparison.COSTS_BETTER1:
                        if keyscmp != PathKeyComparison.PATHKEYS_BETTER2:
                            outercmp = subset_compare(path, old_path)
                            if ((outercmp == SubsetComparison.SETS_EQUAL or 
                                 outercmp == SubsetComparison.SETS_SUBSET1) and
                                 (new_rows <= old_rows)):
                                remove_old = True   # new dominates old 
                        break
                    case PathCostComparison.COSTS_BETTER2:
                        if keyscmp != PathKeyComparison.PATHKEYS_BETTER1:
                            outercmp = subset_compare(path, old_path)
                            if ((outercmp == SubsetComparison.SETS_EQUAL or 
                                 SubsetComparison.SETS_SUBSET2) and 
                                 (new_rows >= old_rows)):
                                accept_new = False  # old dominates new
                        break
                    case PathCostComparison.COSTS_DIFFERENT:
                        # cannot happen, but safety for compiling
                        break
        
        # from here on we insert and remove dominating and not dominating paths within the pathlist 
        if remove_old: 
            rel.pathlist.pop(i)
        else: 
            if path.estimated_cost >= old_path.estimated_cost:
                insert_at = i + 1

        if not accept_new:
            break
    
    if accept_new:
        rel.pathlist.insert(insert_at, path )




def compare_path_costs_fuzzily(path: QueryPlan, old_path: QueryPlan, STD_FUZZ_FACTOR: float) -> PathCostComparison:
    """
    Compares the estimated costs of two paths, using a fuzzy factor to determine if one path is better than the other.
    
    """
    new_cost = path.estimated_cost
    old_cost = old_path.estimated_cost

    if (new_cost > old_cost * STD_FUZZ_FACTOR):
        return PathCostComparison.COSTS_BETTER2
    if (old_cost > new_cost * STD_FUZZ_FACTOR):
        return PathCostComparison.COSTS_BETTER1
    else: return PathCostComparison.COSTS_EQUAL

    

def compare_pathkeys(pdp: PostgresDynProg, new_keys: Sorting | None , old_keys: Sorting | None) -> PathKeyComparison:
    if pdp._same_sorting(new_keys, other=old_keys):
        return PathKeyComparison.PATHKEYS_EQUAL
    elif pdp._sorting_subsumes(new_keys, other=old_keys):
        return PathKeyComparison.PATHKEYS_BETTER1
    elif pdp._sorting_subsumes(old_keys, other=new_keys): 
        return PathKeyComparison.PATHKEYS_BETTER2
    else: return PathKeyComparison.PATHKEYS_DIFFERENT

def subset_compare(new_path: QueryPlan, old_path: QueryPlan): 
    table_set_new = new_path.tables()
    table_set_old = old_path.tables()

    if table_set_new == table_set_old:
        return SubsetComparison.SETS_EQUAL
    elif table_set_new.issubset(table_set_old):
        return SubsetComparison.SETS_SUBSET1
    elif table_set_old.issubset(table_set_new):
        return SubsetComparison.SETS_SUBSET2
    else: return SubsetComparison.SETS_DIFFERENT
    

