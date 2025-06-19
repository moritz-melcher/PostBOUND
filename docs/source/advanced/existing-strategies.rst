Existing Optimizer Implementations
==================================

PostBOUND comes with some existing optimizer implementations from influential or interesting works of the last couple of
years.
These can be used to compare your novel idea against existing approaches.
In addition to the actual research strategies, there are also some "pseudo-strategies" such as using the native optimizer
of a database system or just randomly deciding.
Currently, the following optimizers are implemented:

.. important::

    We are constantly looking for new contributions to the PostBOUND optimimzer library.
    Our goal is to provide a comprehensive collection of optimizers that can be used for research and benchmarking.
    If you have developed an optimizer prototype we would be happy to include it in PostBOUND.
    Just reach out to us at our `GitHub repository <https://github.com/rbergm/PostBOUND>`_ or by emailing us at
    `rico.bergmann1@tu-dresden.de <mailto:rico.bergmann1@tu-dresden.de>`_.

+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+
| Name         | Description                                                                                                                                                                 | Reference        | Package                                                         |
+==============+=============================================================================================================================================================================+==================+=================================================================+
| UES          | Upper-bound driven join order optimizer. Bounds are derived from base statistics, specifically most-common values.                                                          | [Hertzschuch21]_ | :mod:`ues <postbound.optimizer.strategies.ues>`.                |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+
| TONIC        | Learned physical operator selection. Operators are selected based on past experience and optional pretraining. Learning utilizes a prefix tree instead of a neural network. | [Hertzschuch22]_ | :mod:`tonic <postbound.optimizer.strategies.tonic>`             |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+
| DP           | Dynamic programming-based join order optimizer, with an alternative algorithm that mimics the actual Postgres enumerator.                                                   |                  | :mod:`dynprog <postbound.optimizer.strategies.dynprog>`         |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+
| *native*     | Native optimization uses the built-in query optimizer of a database system.                                                                                                 |                  | :mod:`native <postbound.optimizer.strategies.native>`           |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+
| *random*     | Random selection of join orders and physical operators.                                                                                                                     |                  | :mod:`randomized <postbound.optimizer.strategies.randomized>`   |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+
| *exhaustive* | These algorithms do not actually select a plan, but rather enumerate all possible plans (or join orders, or operator assignments).                                          |                  | :mod:`enumeration <postbound.optimizer.strategies.enumeration>` |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-----------------------------------------------------------------+

.. [Hertzschuch21]
    Axel Hertzschuch, Claudio Hartmann, Dirk Habich and Wolfgang Lehner:
    "*Simplicity Done Right for Join Ordering*"
    CIDR 2021 (Link: https://www.cidrdb.org/cidr2021/papers/cidr2021_paper01.pdf)

.. [Hertzschuch22]
    Axel Hertzschuch, Claudio Hartmann, Dirk Habich and Wolfgang Lehner:
    "*Turbo-Charging SPJ Query Plans with Learned Physical Join Operator Selections*"
    VLDB 2022 (DOI: https://doi.org/10.14778/3551793.3551825)
