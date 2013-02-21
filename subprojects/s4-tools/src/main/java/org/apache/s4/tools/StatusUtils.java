package org.apache.s4.tools;

import java.util.Collection;
import java.util.Map;

import com.google.common.base.Strings;

/**
 * Some utility classes for formatting the output of Status tools
 * 
 */
public class StatusUtils {

    /**
     * show as cluster1(app1), cluster2(app2)
     * 
     * @param clusters
     *            cluster list
     * @param clusterAppMap
     *            <cluster,app>
     * @return
     */
    public static String getFormatString(Collection<String> clusters, Map<String, String> clusterAppMap) {
        if (clusters == null || clusters.size() == 0) {
            return StatusUtils.NONE;
        } else {
            // show as: cluster1(app1), cluster2(app2)
            StringBuilder sb = new StringBuilder();
            for (String cluster : clusters) {
                String app = clusterAppMap.get(cluster);
                sb.append(cluster);
                if (!StatusUtils.NONE.equals(app)) {
                    sb.append("(").append(app).append(")");
                }
                sb.append(" ");
            }
            return sb.toString();
        }
    }

    public static String title(String content, char highlighter, int width) {
        return Strings.repeat(String.valueOf(highlighter), ((width - content.length()) / 2)) + content
                + Strings.repeat(String.valueOf(highlighter), ((width - content.length()) / 2));
    }

    public static String noInfo(String content) {
        return StatusUtils.inMiddle("---- " + content + " ----", 130) + "\n\n";
    }

    public static String inMiddle(String content, int width) {
        int i = (width - content.length()) / 2;
        return String.format("%" + i + "s%s", " ", content);
    }

    public static String generateEdge(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("-");
        }
        return sb.toString();
    }

    static String NONE = "--";

}
