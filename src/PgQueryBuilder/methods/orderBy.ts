import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  AllowedFields,
  GroupByFields,
  ORDER_BY,
  PreparedValues,
} from '../internalTypes';
import { getFieldValue } from './fieldFunc';
import { attachArrayWith } from './helperFunction';
import { ensureArray, isNullableValue, isValidArray } from './util';

export class OrderByQuery {
  static prepareOrderByQuery<Model>(
    allowedFields: AllowedFields,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    orderBy?: ORDER_BY<Model>,
    isAggregateAllowed = true,
    isExistsFilter = false,
  ) {
    if (!isValidArray(orderBy)) return '';
    const orderStatement: string[] = [DB_KEYWORDS.orderBy];
    const rawOrderBy = orderBy
      .map((o) => {
        const [col, order = 'DESC', nullOption] = ensureArray(o);
        const orders: string[] = [];
        const val = getFieldValue(
          null,
          col,
          preparedValues,
          groupByFields,
          allowedFields,
          { isExistsFilter, isAggregateAllowed, treatStrAsCol: true },
        );
        if (isNullableValue(val)) {
          return null;
        }
        orders.push(val);
        orders.push(order as string, (nullOption as string) || '');
        return attachArrayWith.space(orders);
      })
      .filter(Boolean);
    const qry = attachArrayWith.comaAndSpace(rawOrderBy);
    orderStatement.push(qry);
    return attachArrayWith.space(orderStatement);
  }
}
