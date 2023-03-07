import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.utils.SdkAutoCloseable;

public class sdk_version {

    public static void main(String[] args) {

        // This code snippet temporarily overrides the system property that specifies the AWS SDK version
        // to force the SDK to use the latest available version.
        try (SdkAutoCloseable sdkAutoCloseable = SdkSystemSetting.overrideSdkVersion(null)) {
            String version = software.amazon.awssdk.core.SdkVersion.getVersion();
            System.out.println("AWS SDK version: " + version);
        } catch (Exception e) {
            System.out.println("Error getting AWS SDK version: " + e.getMessage());
        }
    }
}
