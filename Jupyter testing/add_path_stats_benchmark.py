import postbound as pb
from postbound.optimizer.strategies.dynprog import PostgresDynProg
from add_path_hook import postgres_add_path_hook_standard
from add_path_hook import postgres_add_path_hook_1
from postbound.experiments import prepare_export
import warnings

warnings.filterwarnings("ignore", category=UserWarning)


postgres_instance = pb.postgres.connect(connect_string="dbname=stats user=postbound host=localhost")
stats_workload = pb.workloads.stats()

# there is a bug with query 58 of the stats workload so we filter it
bad = {"q-57", "q-58", "q-60", "q-72"}
stats_workload = stats_workload.filter_by(lambda label, _q: label not in bad)


# Create the optimization pipeline
optimization_pipeline_1 = pb.TextBookOptimizationPipeline(postgres_instance)
optimization_pipeline_2 = pb.TextBookOptimizationPipeline(postgres_instance)
# Create a dynamic programming optimizer instance with wanted add_path hook
dynprog_1 = PostgresDynProg(
    enable_memoize=False,
    add_path_hook= postgres_add_path_hook_standard,
    target_db=postgres_instance
    )
optimization_pipeline_1.setup_plan_enumerator(dynprog_1)
optimization_pipeline_1.build()
# Create the dynamic programming optimizer instance with add_path hook variant including pruning
from add_path_hook import postgres_add_path_hook_1
dynprog_2 = PostgresDynProg(
    enable_memoize=False,
    add_path_hook= postgres_add_path_hook_1,
    target_db=postgres_instance
)
# include query preparation 
optimization_pipeline_2.setup_plan_enumerator(dynprog_2)
optimization_pipeline_2.build()
query_preparation = pb.executor.QueryPreparationService(prewarm=True)

# native results
native_results = pb.execute_workload(stats_workload, postgres_instance, workload_repetitions=1, 
                                     per_query_repetitions=1, shuffled=False,
                                     query_preparation=query_preparation, include_labels=True, logger="tqdm")

standard_results = pb.optimize_and_execute_workload(stats_workload, optimization_pipeline_1, workload_repetitions=1,
                                                    per_query_repetitions=1, shuffled=False,
                                                    query_preparation=query_preparation, include_labels=True, logger="tqdm")

pruning_results = pb.optimize_and_execute_workload(stats_workload, optimization_pipeline_2, workload_repetitions=1,
                                                  per_query_repetitions=1, shuffled=False,
                                                  query_preparation=query_preparation, include_labels=True, logger="tqdm")


prepare_export(native_results)
prepare_export(standard_results)
prepare_export(pruning_results)
native_results.to_csv("native_stats_results.csv")
standard_results.to_csv("standard_stats_results.csv")
pruning_results.to_csv("pruning_stats_results.csv")