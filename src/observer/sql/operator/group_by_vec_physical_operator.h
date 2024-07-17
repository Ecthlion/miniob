/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions){};

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override ;
private:
    std::vector<std::unique_ptr<Expression>> group_by_exprs_;
    std::vector<Expression *> aggregate_expressions_;  /// 聚合表达式
    std::vector<Expression *> value_expressions_;
    std::unordered_map<std::string, std::vector<void *>> group_by_map_;
    std::vector<std::string> group_keys_;
    size_t current_group_index_ = 0;
};

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
    ASSERT(children_.size() == 1, "group by operator only supports one child");

    PhysicalOperator &child = *children_[0];
    RC rc = child.open(trx);
    if (OB_FAIL(rc)) {
        LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
        return rc;
    }

    Chunk chunk;
    while (OB_SUCC(rc = child.next(chunk))) {
        // 生成 group key
        std::string group_key;
        for (size_t col_idx = 0; col_idx < chunk.column_num(); ++col_idx) {
            Value value = chunk.get_value(col_idx, 0); // 获取第一行的值作为 key
            group_key.append(reinterpret_cast<char*>(&value), sizeof(value));
        }

        // group key 不存在 初始化聚合值
        if (group_by_map_.find(group_key) == group_by_map_.end()) {
            group_by_map_[group_key] = std::vector<void *>(aggregate_expressions_.size(), nullptr);
            for (size_t i = 0; i < aggregate_expressions_.size(); ++i) {
                auto *aggr_expr = static_cast<AggregateExpr *>(aggregate_expressions_[i]);
                if (aggr_expr->aggregate_type() == AggregateExpr::Type::SUM) {
                    if (aggr_expr->value_type() == AttrType::INTS) {
                        void *aggr_value = malloc(sizeof(SumState<int>));
                        ((SumState<int> *)aggr_value)->value = 0;
                        group_by_map_[group_key][i] = aggr_value;
                    } else if (aggr_expr->value_type() == AttrType::FLOATS) {
                        void *aggr_value = malloc(sizeof(SumState<float>));
                        ((SumState<float> *)aggr_value)->value = 0;
                        group_by_map_[group_key][i] = aggr_value;
                    } else {
                        ASSERT(false, "not supported value type");
                    }
                } else {
                    ASSERT(false, "not supported aggregation type");
                }
            }
            group_keys_.push_back(group_key);
        }

        // 更新 aggregate values
        for (size_t i = 0; i < aggregate_expressions_.size(); ++i) {
            auto *aggr_expr = static_cast<AggregateExpr *>(aggregate_expressions_[i]);
            const Column &column = chunk.column(i); // 获取整个列

            if (aggr_expr->aggregate_type() == AggregateExpr::Type::SUM) {
                if (aggr_expr->value_type() == AttrType::INTS) {
                    SumState<int> *state_ptr = reinterpret_cast<SumState<int> *>(group_by_map_[group_key][i]);
                    int *data = (int *)column.data();
                    for (size_t j = 0; j < column.count(); ++j) {
                        state_ptr->value += data[j];
                    }
                } else if (aggr_expr->value_type() == AttrType::FLOATS) {
                    SumState<float> *state_ptr = reinterpret_cast<SumState<float> *>(group_by_map_[group_key][i]);
                    float *data = (float *)column.data();
                    for (size_t j = 0; j < column.count(); ++j) {
                        state_ptr->value += data[j];
                    }
                } else {
                    ASSERT(false, "not supported value type");
                }
            } else {
                ASSERT(false, "not supported aggregation type");
            }
        }
    }

    if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS;
    }

    return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
    if (current_group_index_ >= group_keys_.size()) {
        return RC::RECORD_EOF;
    }

    const std::string &group_key = group_keys_[current_group_index_++];
    const auto &aggr_values = group_by_map_[group_key];

    chunk.reset_data(); // 重置 Chunk 数据

    for (size_t i = 0; i < aggregate_expressions_.size(); ++i) {
        auto *aggr_expr = static_cast<AggregateExpr *>(aggregate_expressions_[i]);
        std::unique_ptr<Column> column = std::make_unique<Column>();

        if (aggr_expr->aggregate_type() == AggregateExpr::Type::SUM) {
            if (aggr_expr->value_type() == AttrType::INTS) {
                int value = ((SumState<int> *)aggr_values[i])->value;
                column->append(reinterpret_cast<char*>(&value), 1);
            } else if (aggr_expr->value_type() == AttrType::FLOATS) {
                float value = ((SumState<float> *)aggr_values[i])->value;
                column->append(reinterpret_cast<char*>(&value), 1);
            } else {
                ASSERT(false, "not supported value type");
            }
        } else {
            ASSERT(false, "not supported aggregation type");
        }

        chunk.add_column(std::move(column), i);
    }

    return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::close()
{
    for (const auto &pair : group_by_map_) {
        for (void *value : pair.second) {
            free(value);
        }
    }
    group_by_map_.clear();
    group_keys_.clear();
    current_group_index_ = 0;
    return RC::SUCCESS;
}

