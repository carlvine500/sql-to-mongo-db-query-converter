package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StreamProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import sun.reflect.misc.ReflectUtil;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.SQLOutput;
import java.util.*;

public class ParserTest {
    public static void main(String[] args) {
        System.out.println("Hello World!");
// TODO 需要完成 表名.字段名 的提示,后端需要支持它

//        String sql = "SELECT t.ca1.x1, t.ca1 FROM table1 t";
//        String sql = "SELECT *FROM table1 t";
//         String sql = "SELECT t.parentOpty, t.modifiedTime,p.id from Opportunity t,parentOpt p where t.pid=p.id and t.pid=1001";
//         String sql = "SELECT t.parentOpty.id, t.modifiedTime from Opportunity t";
//        String sql = "SELECT Opportunity.id ,Opportunity.Address.id.name,Opportunity.parentOpt.company.name.id from Opportunity";
//        String sql = "SELECT Opportunity.id ,Opportunity.*,Opportunity.address.*,Opportunity.address.dept.*,* from Opportunity";
//        String sql = "SELECT Opportunity.address.dept.* from Opportunity";
        String sql = "select t.id from TenantBO t where t.companyId=\"735\"";
//        String sql = "select data_.a11,11, NVL( (SELECT 1 FROM DUAL), 1) AS A from TEST1,test";
//        String sql = "select convert(t.standard.a1),t.standard.data.fff.* from my_table t where date(column,'YYY-MM-DD') >= '2016-12-12'";
//        String sql = "/*xxx*/select meta(t.standard.data.\"*\"),t.standard.data.fff.* from my_table t where date(column,'YYY-MM-DD') >= '2016-12-12' --note";
//        String sql = "desc xxx";
//        String sql = "select a,b,c,d,date(e),now(),xxx.* from my_table ";
//        String sql = "select a.b.c.d.e.f from my_table where a=\"1\"";
//        String sql = "select a from Radar where a=\"1089163\" ";
//        String sql = "select a,b,c,d,date(e),now() from my_table where (a>1 and b>2) or c>3 order by abc";
//        String sql = "select distinct(r) from my_table where (a>1 and b>2) or c>3 group by acc order by abc";

        // QueryConverter.write拼拼的mongo json
        // QueryConverter.run执行
        // QueryConverter.getMongoQueryInternal
        // SqlUtils.isDateFunction
        try {
            byte[] bytes = sql.getBytes(Charsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(bytes);
            StreamProvider streamProvider = new StreamProvider(inputStream, Charsets.UTF_8.name());
            CCJSqlParser jSqlParser = new CCJSqlParser(streamProvider);
            Statement statement = jSqlParser.Statement();
            final PlainSelect plainSelect = (PlainSelect) (((Select) statement).getSelectBody());
            System.out.println("sql==>"+statement);
            Map<String,List<String>> tableColumns = new HashMap<>();
            tableColumns.put("Opportunity",Arrays.asList("id","name"));
            tableColumns.put("address",Arrays.asList("code","name"));
            tableColumns.put("dept",Arrays.asList("dName","dCode"));
            // selectItems 碰到*号 需要展开.
            List<SelectExpressionItem> selectItems = new ArrayList<>();
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                if(selectItem instanceof AllColumns ){
                    AllColumns a =  (AllColumns) selectItem;
                    String tName = ((Table)plainSelect.getFromItem()).getName();
                    List<String> columns = tableColumns.get(tName);
                    for (String columnName : columns) {
                        Column column = new Column(new Table(tName),columnName);
                        SelectExpressionItem e = new SelectExpressionItem(column);
                        selectItems.add(e);
                    }
                }else if(selectItem instanceof AllTableColumns ){
                    AllTableColumns a =  (AllTableColumns) selectItem;
                    String fullyQualifiedName = a.getTable().getFullyQualifiedName();
                    Table allTable = a.getTable();
                    String tName = allTable.getName();
                    String fullTableName = StringUtils.substringBefore(fullyQualifiedName, "*");
                    List<String> partItems = Arrays.asList(StringUtils.split(fullTableName,"."));
                    List<String> columns = tableColumns.get(tName);
                    for (String columnName : columns) {
                        Table table = new Table(partItems);
                        Column column = new Column(table,columnName);
                        SelectExpressionItem e = new SelectExpressionItem(column);
                        selectItems.add(e);
                    }
                }
            }
            plainSelect.getSelectItems().addAll(selectItems);

            // 复制名称,跳过*号
            List<String> selectColumns = new ArrayList<>();
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                if (!(selectItem instanceof SelectExpressionItem)) {
                    continue;
                }
                selectColumns.add(selectItem.toString());
            }
            System.out.println("old:" + selectColumns);
            //target: Opportunity.Address.id
            //step1: Opportunity.Address
            //step2: 表达式服务中查询对象Opportunity.Address的属性  Opportunity.Address.id
            //改select字段名
//            for (SelectItem selectItem : plainSelect.getSelectItems()) {
//                if (!(selectItem instanceof SelectExpressionItem)) {
//                    continue;
//                }
//                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
//                if (expression instanceof Column) {
//                    Column column = (Column) expression;
//                    Table table = column.getTable();
//                        List<String> partItems = getFieldValue(table, "partItems");
//                        if (partItems.size() >= 2) {
//                            String realTableName = partItems.get(partItems.size() - 1);
//                            String realColumnName = partItems.get(partItems.size() - 2);
//                            setFieldValue(table, "partItems", Arrays.asList(realTableName));
//                            column.setColumnName(realColumnName);
//                        }
//                }
//            }
//            Table fromTable = (Table) plainSelect.getFromItem();
//            String tableName = fromTable.getName();
//            String alias = plainSelect.getFromItem().getAlias().getName();
//            Map<String/*alias*/,String/*tableName*/> aliasTableNames = new HashMap<>();
//            aliasTableNames.put(alias,tableName);
//            for (SelectItem selectItem : plainSelect.getSelectItems()) {
//                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
//                Column c =  (Column) expression;
//                Table table = c.getTable();
//                String fullName = aliasTableNames.get(table.getName());
//                if (StringUtils.isNotBlank(fullName)) {
//                    table.setName(fullName);
//                }
//            }
//            fromTable.setName(tableName);
//            plainSelect.getWhere();
            java.util.function.Consumer<Expression> renameColumnFunc = e -> {
                if (e instanceof Column) {
                    Column col = (Column) e;
                    col.setColumnName(col.getColumnName() + "_new");
                }
            };
            java.util.function.Consumer<Table> renameTableNameFunc = e -> {
                e.setName(e.getName() + "_new");
            };
            //改where字段名
            if (plainSelect.getWhere() != null) {
                plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(Column column) {
                        renameColumnFunc.accept(column);
                        super.visit(column);
                    }
                });
            }

            //改表名
            plainSelect.accept(new SelectDeParser() {
                @Override
                public void visit(Table tableName) {
                    renameTableNameFunc.accept(tableName);
                    super.visit(tableName);
                }
            });

            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                if (!(selectItem instanceof SelectExpressionItem)) {
                    continue;
                }
                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                if (expression instanceof Column) {
                    renameColumnFunc.accept(expression);
                } else if (expression instanceof Parenthesis) {
                    renameColumnFunc.accept(((Parenthesis) expression).getExpression());
                } else {
                    if (expression instanceof Function) {
                        Function function = (Function) expression;
                        ExpressionList parameters = function.getParameters();
                        if (parameters == null) {
                            continue;
                        }
                        for (Expression funcExpr : parameters.getExpressions()) {
                            renameColumnFunc.accept(funcExpr);
                        }
                    }
                }
            }
            if (plainSelect.getOrderByElements() != null) {
                for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                    orderByElement.accept(new OrderByVisitor() {
                        @Override
                        public void visit(OrderByElement orderBy) {
                            renameColumnFunc.accept(orderBy.getExpression());
                        }
                    });
                }
            }

            if (plainSelect.getGroupByColumnReferences() != null) {
                for (Expression expression : plainSelect.getGroupByColumnReferences()) {
                    renameColumnFunc.accept(expression);
                }
            }

            FromItem fromItem = plainSelect.getFromItem();
            System.out.println("getFromItem==>" + fromItem);
            if (plainSelect.getWhere() != null) {
                System.out.println(plainSelect.getWhere());
            }
            System.out.println("getSelectItems==>" + JSON.toJSONString(plainSelect.getSelectItems().toString()));
            if (plainSelect.getOrderByElements() != null) {
                System.out.println(JSON.toJSONString(plainSelect.getOrderByElements().toString()));
            }
            if (plainSelect.getGroupByColumnReferences() != null) {
                System.out.println(JSON.toJSONString(plainSelect.getGroupByColumnReferences().toString()));
            }

//            System.out.println("selectItemsOld:" + selectItemsOld);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static <T> T getFieldValue(Object instance, String fieldName) throws IllegalAccessException, NoSuchFieldException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(instance);
    }

    public static void setFieldValue(Object instance, String fieldName, Object value) throws IllegalAccessException, NoSuchFieldException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }


}