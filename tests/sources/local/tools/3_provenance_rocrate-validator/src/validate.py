
# Import the `services` and `models` module from the rocrate_validator package
from rocrate_validator import services, models
import sys


def configure_validation(crate, profile_string):
    # Create an instance of `ValidationSettings` class to configure the validation
    settings = services.ValidationSettings(
        # Set the path to the RO-Crate root directory
        data_path = crate,
        # Set the identifier of the RO-Crate profile to use for validation.
        # If not set, the system will attempt to automatically determine the appropriate validation profile.
        profile_identifier=profile_string,
        # Set the requirement level for the validation
        requirement_severity=models.Severity.REQUIRED,
    )
    return settings


def main():
    args = sys.argv[1:]
    crate = str(args[0])

    # Call the validation service with the settings
    issues = False
    for prof_str in ['ro-crate-1.1', 'workflow-ro-crate-1.0', 'process-run-crate-0.5', 'workflow-run-crate-0.5']:  # , 'provenance-run-crate-0.5']:
        settings = configure_validation(crate, prof_str)
        result = services.validate(settings)

        # Check if the validation was successful
        if not result.has_issues():
            print(f"RO-Crate is a valid {prof_str}!")
        else:
            print(f"RO-Crate is an invalid {prof_str}!")
            issues = True
            # Explore the issues
            for issue in result.get_issues():
                # Every issue object has a reference to the check that failed, the severity of the issue, and a message describing the issue.
                print(f"Detected issue of severity {issue.severity.name} with check \"{issue.check.identifier}\": {issue.message}")

    if issues:
        exit(1)

if __name__ == "__main__":
    main()


