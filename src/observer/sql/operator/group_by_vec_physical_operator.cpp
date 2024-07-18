/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"


RC GroupByVecPhysicalOperator::open(Trx *trx)
{

    PhysicalOperator &child = *children_[0];
    RC                rc    = child.open(trx);

    while (OB_SUCC(rc = child.next(chunk_))) {
        Chunk group_chunk;
        Chunk aggregate_chunk;
        for (size_t group_by_idx = 0; group_by_idx < groupby_exprs_.size(); ++group_by_idx){
            Column column_1;
            groupby_exprs_[group_by_idx]->get_column(chunk_, column_1);
            group_chunk.add_column(make_unique<Column>(groupby_exprs_[group_by_idx]->value_type(),groupby_exprs_[group_by_idx]->value_length()), groupby_exprs_[group_by_idx]->pos());
            output_chunk_.add_column(make_unique<Column>(groupby_exprs_[group_by_idx]->value_type(),groupby_exprs_[group_by_idx]->value_length()), group_by_idx);
        }

        for (size_t aggr_idx = 0; aggr_idx < value_expressions_.size(); aggr_idx++) {
            Column column_2;
            value_expressions_[aggr_idx]->get_column(chunk_, column_2);
            aggregate_chunk.add_column(make_unique<Column>(value_expressions_[aggr_idx]->value_type(),value_expressions_[aggr_idx ]->value_length()), value_expressions_[aggr_idx]->pos());
            output_chunk_.add_column(make_unique<Column>(value_expressions_[aggr_idx]->value_type(),value_expressions_[aggr_idx ]->value_length()), aggr_idx + groupby_exprs_.size());
        }
        hashtable->add_chunk(group_chunk, aggregate_chunk);
    }
    scanner->open_scan();

    return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{

    RC rc = scanner->next(chunk);
    return rc;
}

RC GroupByVecPhysicalOperator::close()
{
    children_[0]->close();
    return RC::SUCCESS;
}
