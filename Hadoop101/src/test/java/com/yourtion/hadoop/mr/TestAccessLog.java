package com.yourtion.hadoop.mr;

import eu.bitwalker.useragentutils.UserAgent;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestAccessLog {

    private int findStringIndex(String text, String wordToFind, int skip) {
        Pattern word = Pattern.compile(wordToFind);
        Matcher match = word.matcher(text);

        int c = 0;
        while (match.find()) {
            c += 1;
            if (c == skip) {
                return match.start();
            }
        }
        return -1;
    }

    @Test
    public void TestParseUA() {
        String log = "66.133.109.36 - - [03/Oct/2018:13:20:14 +0000] \"GET /.well-known/acme-challenge/5rPc4i1ZLhNdxGgc7g30Otika5oAh6HMv6l6a4RB16I HTTP/1.1\" 200 87 \"-\" \"Mozilla/5.0 (compatible; Let's Encrypt validation server; +https://www.letsencrypt.org)\" \"-\"";
        int idx = findStringIndex(log, "\"", 5);
        int end = findStringIndex(log, "\"", 6);
        String usStr = log.substring(idx + 1, end);
        UserAgent ua = UserAgent.parseUserAgentString(usStr);
        System.out.println(ua.getBrowser());
    }
}
