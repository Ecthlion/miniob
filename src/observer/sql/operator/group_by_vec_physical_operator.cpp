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
    ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

    PhysicalOperator &child = *children_[0];
    RC                rc    = child.open(trx);
    if (OB_FAIL(rc)) {
        LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
        return rc;
    }
    Chunk groups_chunk;
    Chunk aggrs_chunk;

    for(auto g_idx=0;g_idx<aggregate_expressions_.size();g_idx++) {
        Column column;

    }


    for(auto v_idx=0;v_idx<value_expressions_.size();v_idx++) {

    }



    return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
    output_chunk_.reset_data();
    chunk.reset();
    RC rc = scanner->next(output_chunk_);
    chunk.reference(output_chunk_);
    return rc;
}


RC GroupByVecPhysicalOperator::close()
{
    children_[0]->close();
    LOG_INFO("close group by operator");
    return RC::SUCCESS;
}