package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SQLCommandInfoHolder {
    private final SQLCommandType sqlCommandType;
    private final boolean isDistinct;
    private final boolean isCountAll;
    private final String table;
    private final long limit;
    private final Expression whereClause;
    private final List<SelectItem> selectItems;
    private final List<Join> joins;
    private final List<String> groupBys;
    private final List<OrderByElement> orderByElements;
    private final Statement statement;

    public SQLCommandInfoHolder(SQLCommandType sqlCommandType, Expression whereClause,
                                boolean isDistinct, boolean isCountAll, String table, long limit, List<SelectItem> selectItems, List<Join> joins, List<String> groupBys, List<OrderByElement> orderByElements
            , Statement statement) {
        this.sqlCommandType = sqlCommandType;
        this.whereClause = whereClause;
        this.isDistinct = isDistinct;
        this.isCountAll = isCountAll;
        this.table = table;
        this.limit = limit;
        this.selectItems = selectItems;
        this.joins = joins;
        this.groupBys = groupBys;
        this.orderByElements = orderByElements;
        this.statement = statement;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    public String getTable() {
        return table;
    }

    public long getLimit() {
        return limit;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public List<String> getGoupBys() {
        return groupBys;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    public Statement getStatement() {
        return statement;
    }

    public static class Builder {
        private final FieldType defaultFieldType;
        private final Map<String, FieldType> fieldNameToFieldTypeMapping;
        private SQLCommandType sqlCommandType;
        private Expression whereClause;
        private boolean isDistinct = false;
        private boolean isCountAll = false;
        private String table;
        private long limit = -1;
        private List<SelectItem> selectItems = new ArrayList<>();
        private List<Join> joins = new ArrayList<>();
        private List<String> groupBys = new ArrayList<>();
        private List<OrderByElement> orderByElements1 = new ArrayList<>();
        private Statement statement;


        private Builder(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping){
            this.defaultFieldType = defaultFieldType;
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        }

        public Builder setJSqlParser(CCJSqlParser jSqlParser) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
           return setJSqlParser(jSqlParser, null, null);
        }

        public Builder setJSqlParser(CCJSqlParser jSqlParser, Consumer<Table> renameTableFunc, BiConsumer<String,Column> renameColumnFunc) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
            final Statement statement = jSqlParser.Statement();
            this.statement = statement;
            if (Select.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.SELECT;
                final PlainSelect plainSelect = (PlainSelect)(((Select)statement).getSelectBody());
                SqlUtils.isTrue(plainSelect != null, "could not parseNaturalLanguageDate SELECT statement from query");
                SqlUtils.isTrue(plainSelect.getFromItem()!=null,"could not find table to query.  Only one simple table name is supported.");
                whereClause = plainSelect.getWhere();
                isDistinct = (plainSelect.getDistinct() != null);
                isCountAll = SqlUtils.isCountAll(plainSelect.getSelectItems());
                SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
                table = plainSelect.getFromItem().toString();
                limit = SqlUtils.getLimit(plainSelect.getLimit());
                orderByElements1 = plainSelect.getOrderByElements();
                selectItems = plainSelect.getSelectItems();
                joins = plainSelect.getJoins();
                groupBys = SqlUtils.getGroupByColumnReferences(plainSelect);
                SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
                renameTableAndColumn(plainSelect,renameTableFunc,renameColumnFunc);
            } else if (Delete.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.DELETE;
                Delete delete = (Delete)statement;
                SqlUtils.isTrue(delete.getTables().size() == 0, "there should only be on table specified for deletes");
                table = delete.getTable().toString();
                whereClause = delete.getWhere();
            }
            return this;
        }

        public void renameTableAndColumn(PlainSelect plainSelect,Consumer<Table> renameTableFunc, BiConsumer<String,Column> renameColumnFunc) {
            if (renameTableFunc == null || renameColumnFunc == null) {
                return;
            }
            //改where字段名
            if(plainSelect.getWhere()!=null){
                plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(Column column) {
                        renameColumnFunc.accept(table,column);
                        super.visit(column);
                    }
                });
            }

            //改表名
            plainSelect.accept(new SelectDeParser() {
                @Override
                public void visit(Table tableName) {
                    renameTableFunc.accept(tableName);
                    super.visit(tableName);
                }
            });
            //改select字段名==>全部查询出来
            List<SelectItem> otherSelectItems = new ArrayList<>();
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                if (selectItem instanceof SelectExpressionItem) {
                    Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                    if (expression instanceof Column) {
                        renameColumnFunc.accept(table,(Column) expression);
                    } else if (expression instanceof Parenthesis) {
                        renameIfItIsColumn(table,((Parenthesis) expression).getExpression(), renameColumnFunc);
                    } else {
                        if (expression instanceof Function) {
                            Function function = (Function) expression;
                            ExpressionList parameters = function.getParameters();
                            if (parameters == null) {
                                continue;
                            }
                            for (Expression funcExpr : parameters.getExpressions()) {
                                renameIfItIsColumn(this.table,funcExpr, renameColumnFunc);
                            }
                        }
                    }
                } else if (selectItem instanceof AllTableColumns) {
                    Column column = new Column(selectItem.toString());
                    renameColumnFunc.accept(this.table,column);
                    SelectExpressionItem e = new SelectExpressionItem(column);
                    otherSelectItems.add(e);
                }
            }
            plainSelect.getSelectItems().addAll(otherSelectItems);
            // rename order by column
            if (plainSelect.getOrderByElements() != null) {
                for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                    orderByElement.accept(new OrderByVisitor() {
                        @Override
                        public void visit(OrderByElement orderBy) {
                            renameIfItIsColumn(table,orderBy.getExpression(),renameColumnFunc);
                        }
                    });
                }
            }
            // rename group by column
            if (plainSelect.getGroupByColumnReferences() != null) {
                for (Expression expression : plainSelect.getGroupByColumnReferences()) {
                    renameIfItIsColumn(this.table,expression,renameColumnFunc);
                }
            }

        }

        public  void renameIfItIsColumn(String table,Object column, BiConsumer<String,Column> renameColumnFunc) {
            if (column instanceof Column) {
                renameColumnFunc.accept(this.table,(Column)column);
            }
        }

        public SQLCommandInfoHolder build() {
            return new SQLCommandInfoHolder(sqlCommandType, whereClause,
                    isDistinct, isCountAll, table, limit, selectItems, joins, groupBys, orderByElements1, statement);
        }

        public static Builder create(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
            return new Builder(defaultFieldType, fieldNameToFieldTypeMapping);
        }
    }
}
