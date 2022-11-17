package com.xzz.app.function;

import com.xzz.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author 徐正洲
 * @create 2022-11-16 15:55
 * <p>
 * 自定义UDTF函数 炸裂函数
 */


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
        List<String> list = null;
        try {
            list = KeyWordUtil.splitKeyWord(str);
            for (String s : list) {
                collect(Row.of(s));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}