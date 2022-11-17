package com.xzz.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 徐正洲
 * @create 2022-11-16 15:22
 * <p>
 * 分词器工具类
 */
public class KeyWordUtil {
    public static List<String> splitKeyWord(String keyWord) throws IOException {
        //创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        //创建分词对象  ik_smart(true) ik_max_word(false)
        StringReader stringReader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);

        //循环取出切好的词
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            String word = next.getLexemeText();
            list.add(word);

            next = ikSegmenter.next();
        }
        //返回集合
        return list;
    }
}
