---
name: Pull Request template
about: Describe the changes of a Pull Request you want to merge.
title: ''
labels: ''
assignees: ''

---

# Status
**READY / WORK IN PROGRESS / HOLD**


# Description

Please include a summary of the change and which issue is fixed. Please also include relevant motivation and context. List any dependencies that are required for this change.

Fixes #(issue_number)

## Type of change

Please delete options that are not relevant.

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] This change requires a documentation update

## Project impact
List the general project components that this PR will affect:

*

## Related PRs
List related PRs against other branches:

branch | PR
------ | ------
other_pr_production | [link]()
other_pr_master | [link]()


# How Has This Been Tested?

Please describe the tests that you ran to verify your changes. If you haven't added any test and it is relevant provide instructions so we can reproduce.

- [ ] I have added a new test with number:
- [ ] I have modified a test to check this. Test number: 
- [ ] I have tested it manually in a **local environment**.
- [ ] I have tested it manually in a **supercomputer**.

Reproduce instructions:

```bash
git checkout <feature_branch>
```  


# Checklist:

- [ ] My code follows the style guidelines of this project
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] New and existing unit tests pass locally with my changes
- [ ] I have run the script to add headers if new files are present (i.e. `utils/scripts/header_setup/replace_all.sh`)
- [ ] I have rebased my branch before trying to merge.
