package es.bsc.compss.http.master;

import java.util.Map;


public class URLReplacer {

    /**
     * Replace URL parameters in the URL.
     * 
     * @param baseUrl base URL containing tokens to replace
     * @param replaceElements a map containing the token to find and the corresponding value that the token (key) will
     *            be replaced with
     * @param openToken string representing an open token in the baseUrl
     * @param closeToken string representing a close token in the baseUrl
     */
    public static String replaceUrlParameters(String baseUrl, Map<String, String> replaceElements, String openToken,
        String closeToken) {

        if (replaceElements == null || replaceElements.isEmpty()) {
            return baseUrl;
        }

        String result = baseUrl;

        for (final Map.Entry<String, String> entry : replaceElements.entrySet()) {
            final String wordToBeReplaced = entry.getKey().trim();
            final String fullStringToBeReplaced = openToken + wordToBeReplaced + closeToken;
            String replacement = entry.getValue();
            // todo: should we also replace other special characters?
            replacement = replacement.replaceAll("#", "");
            replacement = replacement.replaceAll(" ", "");
            result = result.replaceAll(fullStringToBeReplaced, replacement);
        }
        return result;
    }

    /**
     * Replace task parameters in the JSON payload.
     *
     * @param payload original JSON payload containing parameters to be replaced
     * @param replaceElements a map containing the token to find and the corresponding value that the token (key) will
     *            be replaced with
     * @param openToken string representing an open token in the baseUrl
     * @param closeToken string representing a close token in the baseUrl
     */
    public static String formatPayload(String payload, Map<String, String> replaceElements, String openToken,
        String closeToken) {

        if (replaceElements == null || replaceElements.isEmpty()) {
            return payload;
        }

        String result = payload;

        for (final Map.Entry<String, String> entry : replaceElements.entrySet()) {
            final String wordToBeReplaced = entry.getKey().trim();
            final String fullStringToBeReplaced = openToken + wordToBeReplaced + closeToken;
            String replacement = entry.getValue();
            // remove python string extension #
            if (replacement.startsWith("#")) {
                replacement = replacement.replaceFirst("#", "");
            }
            result = result.replaceAll(fullStringToBeReplaced, replacement);
        }
        return result;
    }
}
