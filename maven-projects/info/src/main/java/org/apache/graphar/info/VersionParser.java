import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionParser {

    public static InfoVersion Parse(final String version_str) {
        try {

            int parsedVersion = parserVersionImpl(version_str);

            List<String> parsedTypes = parseUserDefineTypesImpl(version_str);

            return new InfoVersion(parsedVersion, parsedTypes);
        } catch (RuntimeException e) {

            throw new RuntimeException("Invalid version string: '" + version_str + "'. Details: " + e.getMessage(), e);
        }
    }


    public int parserVersionImpl(final String version_str) {

        if (version_str == null || version_str.isEmpty()) {
            throw new RuntimeException("Invalid version string: input cannot be null or empty.");
        }


        final Pattern version_regex = Pattern.compile("gar/v(\\d+).*");


        final Matcher match = version_regex.matcher(version_str);


        if (match.matches()) {

            if (match.groupCount() != 1) {
                throw new RuntimeException("Invalid version string: " + version_str);
            }

            try {
                return Integer.parseInt(match.group(1));
            } catch (NumberFormatException e) {

                throw new RuntimeException("Invalid version string: Could not parse version number from " + version_str, e);
            }
        } else {

            throw new RuntimeException("Invalid version string: Does not match 'gar/v(\\d+).*' format for " + version_str);
        }
    }


    public List<String> parseUserDefineTypesImpl(final String version_str) {
        List<String> userDefineTypes = new ArrayList<>();


        final Pattern user_define_types_regex = Pattern.compile("gar/v\\d+ *\\((.*)\\).*");
        final Matcher match = user_define_types_regex.matcher(version_str);

        if (match.matches()) {

            if (match.groupCount() != 1) {
                throw new RuntimeException("Invalid version string: " + version_str);
            }

            String typesStr = match.group(1);

            String[] typesArray = typesStr.split(",", -1);

            for (String type : typesArray) {

                String trimmedType = type.trim();

                if (!trimmedType.isEmpty()) {
                    userDefineTypes.add(trimmedType);
                }
            }
        }

        return userDefineTypes;
    }
}
