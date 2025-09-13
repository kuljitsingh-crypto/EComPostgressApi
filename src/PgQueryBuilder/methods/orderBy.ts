import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  AllowedFields,
  GroupByFields,
  ORDER_BY,
  PreparedValues,
} from '../internalTypes';
import {
  attachArrayWith,
  ensureArray,
  fieldQuote,
  isCallableColumn,
  isNonEmptyString,
  isValidArray,
  isValidSubQuery,
  validCallableColCtx,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

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
        if (isNonEmptyString(col)) {
          orders.push(fieldQuote(allowedFields, col));
        } else if (isCallableColumn(col)) {
          const { col: finalCol } = validCallableColCtx(col, {
            allowedFields,
            isAggregateAllowed,
            preparedValues,
            groupByFields,
          });
          orders.push(finalCol);
        } else if (isValidSubQuery(col)) {
          orders.push(
            QueryHelper.otherModelSubqueryBuilder(
              '',
              preparedValues,
              groupByFields,
              col,
              { isExistsFilter },
            ),
          );
        } else {
          return null;
        }
        orders.push(order as string, (nullOption as string) || '');
        return attachArrayWith.space(orders);
      })
      .filter(Boolean);
    const qry = attachArrayWith.comaAndSpace(rawOrderBy);
    orderStatement.push(qry);
    return attachArrayWith.space(orderStatement);
  }
}
