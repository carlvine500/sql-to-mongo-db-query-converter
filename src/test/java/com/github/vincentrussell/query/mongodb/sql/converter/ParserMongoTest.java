package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by tingfeng on 2018/10/29.
 */
public class ParserMongoTest {
    public static void main(String[] args) throws ParseException, IOException {
        Map<String, String> columnMapping = new HashMap<>();
        columnMapping.put("a", "1001");
        columnMapping.put("b", "1002");
        columnMapping.put("c", "1003");
        columnMapping.put("d", "1004");
        columnMapping.put("e", "1005");
        Map<String, String> tableMapping = new HashMap<>();
        tableMapping.put("tb_test", "2355");
        // CQL - click query language
//        String sql = "select a,b,c.x,d.c.e.f.g,d.x,e.* from tb_test order by a ,b ,c";
        String sql = "select a,b,c.x,d.c.e.f.g,d.x,e.* from tb_test where (a.id='1' or a.id='2' and a.dept='1002' ) limit 10,11";
//        String sql = "select unknownColumnName from unknownTable where unknownColumnName=\"1089163\" ";
//        String sql = "update unknownColumnName set unknownColumnName=\"1089163\" where unknownColumnName=\"1089163\" ";
//        String sql = "select  a.b.c.d.e.f from tb_test";
//        String sql = "select i,u, t.* from tb_test";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8));
        Set<String> subObjects = new HashSet<>();
        java.util.function.BiConsumer<String,Column> renameColumnFunc = (t,e) -> {
            String fullName = e.getFullyQualifiedName();
            String columnName = e.getColumnName();
            if (StringUtils.contains(fullName, ".")) {
                columnName = StringUtils.substringBefore(fullName, ".");
                e.setTable(null);
                subObjects.add(fullName);
            }
            String newName = columnMapping.get(columnName);
            if (newName != null) {
                e.setColumnName("data."+newName);
            }
        };
        java.util.function.Consumer<Table> renameTableNameFunc = e -> {
            String newName = tableMapping.get(e.getName());
            if (newName != null) {
                e.setName(newName);
            }
        };

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        QueryConverter queryConverter = new QueryConverter(inputStream, Collections.<String, FieldType>emptyMap(), FieldType.UNKNOWN, renameTableNameFunc, renameColumnFunc);

        Statement statement = queryConverter.getSqlCommandInfoHolder().getStatement();
        PlainSelect plainSelect = (PlainSelect) (((Select) statement).getSelectBody());

//        Table table = new Table();
//        Column column = new Column(table, "companyId");
//        SelectExpressionItem item = new SelectExpressionItem(column);

        System.out.println("after add default items:" + plainSelect);

        queryConverter.write(byteArrayOutputStream);
        System.out.println(byteArrayOutputStream.toString());
        System.out.println(JSON.toJSONString(subObjects));
        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            System.out.println(selectItem.toString()+"--->");
        }

//        System.out.println(queryConverter.dryRun());

    }
//        queryConverter.getMongoQuery().get
//        SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
//        selectExpressionItem.setExpression(new Column(new Table(),"xxx"));
//        plainSelect.getSelectItems().add(selectExpressionItem);


}
