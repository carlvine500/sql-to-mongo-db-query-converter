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
public class MongoRunCmdTest {
    public static void main(String[] args) throws ParseException, IOException {

        String sql = "select a,b,c.x,d.c.e.f.g,d.x,e.* from tb_test where (a.id='1' or a.id='2' and a.dept='1002' ) limit 10,11";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8));

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        QueryConverter queryConverter = new QueryConverter(inputStream, Collections.<String, FieldType>emptyMap(), FieldType.UNKNOWN);

        Statement statement = queryConverter.getSqlCommandInfoHolder().getStatement();
        PlainSelect plainSelect = (PlainSelect) (((Select) statement).getSelectBody());

//        Table table = new Table();
//        Column column = new Column(table, "companyId");
//        SelectExpressionItem item = new SelectExpressionItem(column);

        System.out.println("after add default items:" + plainSelect);

        queryConverter.write(byteArrayOutputStream);
        System.out.println(byteArrayOutputStream.toString());
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
