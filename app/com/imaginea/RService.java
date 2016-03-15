package com.imaginea;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Enumeration;

/**
 * Created by sachint on 8/3/16.
 */
public class RService {

    Logger log = LoggerFactory.getLogger("appplication");
    private static Rengine re = new Rengine(new String[]{"--vanilla"}, false, null);
    private static String wcScript = null;

    static{
        InputStream is = RService.class.getClassLoader().getResourceAsStream("RWC.R");

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuffer sb = new StringBuffer();
        String line = "";
        try {
            while((line = br.readLine()) != null){
               sb.append(line);
                sb.append("\n");
            }
            wcScript = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

        }

    }

    public String generateWordCloud(String term, String host, int port) {
        StringBuffer sb = new StringBuffer("library(\"elastic\")\n");
        sb.append("connect(es_base = \"" + host + "\", es_port = " + port + ")\n");
        sb.append("term <- \"" + term + "\" \n");
        sb.append(wcScript);
        REXP exp = evaluateScript(sb.toString());
        System.out.println(exp);
        return exp.toString();
    }


    // Evaluate block of R expressions, taking into account the fact that Rengine only executes
    // one statement at a time.  Unconditionally dumps out lines before executing the script
    // so that if anything goes wrong we can copy paste the constructed output (scriptLines)
    // directly into an R session.
    //
    public REXP evaluateScript(String scriptLines) {
        log.debug("evaluating: " + scriptLines);
        REXP exp = null;
        for (String line: scriptLines.split("\n")) {
            exp = re.eval(line);
        }
        if(exp != null){
            return exp;
        }
        return null;
    }

    public static void shutdown() {
        re.end();
    }


}
