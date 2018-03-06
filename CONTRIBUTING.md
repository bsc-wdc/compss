# Contributing to COMPSs Framework


COMPSs repository consists in two basic branches:

* trunk: development branch where new developments should be merged; can not merge requests (MR) unless tests are passed by gitlab-runner
* stable: nightly builds; contains code that has passed all jenkins tests (ubuntu, centos, suse, debia, and sc)


The general workflow is as follows:

1. Get the most up-to-date trunk version
2. Create a branch with a **relevant feature name** to do your development
3. Do your developments and stuff, **do not attempt to merge until feature is completely finished and tested**.
4. Rebase your branch with trunk before commiting to avoid and solve conflicts
5. Create a merge request (MR) on gitlab. This will trigger the gitlab continuous integration (CI) runner tests
6. If they pass you can merge your work (it will be automerge if you selected the option)
7. Otherwise correct the issue and push the development branch again (this will again trigger the tests).


## Detailed Workflow instructions


Requirements and considerations:

* Git minimum setup: http://gitimmersion.com/lab_01.html
* All guide uses git immersion aliases refer to: http://gitimmersion.com/lab_11.html
* **During all the guide USERNAME and PASSWORD will refer to your LDAP credentials**



### 1. Checkout an up-to-date trunk branch

1.a If you **do not have a cloned repository** yet issue:

`git clone http://USERNAME@compss.bsc.es/compss/framework.git`

(you can remove the `USERNAME@` part, but you will be asked for your username everytime unless configured)

1.b If you **already have a cloned repository**:

````
git co trunk # checkout to trunk branch
git pull origin trunk # pull the changes present in the remote (origin) repository to your branch
```

To make sure you are on trunk branch you can use either:

```
git br  # alias for git branch
git st  # alias for git status
```
### 2. Create a branch for you development

Create a branch and start working on it:

 `git co -b name_of_the_feature_you_are_developing` # co -b is a shorthand for creating and new branch and checking it out
 
 Try to use relevant and clear names for your branches, this way other users can now what is being worked on, and once the branch is merged it will be easy to keep track of the changes in case something breaks.
 
 Again, you can check which branch you are with `git br` or `git st` 
### 3. Do your developments

Git tracks changes not files so you need to add every change you want to commit to the staging area (which are the changes to be commit).

To add a file's changes to the next commit issue. if this is the first time you are adding the file, git will start tracking the file and show any changes made to it with the command `git status` or alias `git st`. To see which branch you are on you 

`git add file_name`

To commit the staged changes issue:

`git ci -m 'Message explaining which change does the commit introduce not the details or files'`

This will commit the changes to your **local history** not the remote repository. Commits should be as frequent as possible to help you track your own work and be able to fastly undo whatever change that is not desired or that introduced some bug. Moving around git history is easy so take advantage of using the commits to isolate changes. For example, if you are doing a hard development that you are not sure is going to be successful (and thus probably discarded) and you spot a spelling error you would like to change, just change that file, add the changes, and commit like `git ci -m 'spelling error in some function'`. 

If you want to share your branch with another user (for testing for example), to back up important work, you need to push your branch to the remote repository. **If you want to finally merge your finished feature, do not push the changes now, proceed to step 4**

`git push origin name_of_the_feature_you_are_developing`

### 4. Rebase your branch

While you work on your branch the tip of the trunk branch (latest trunk commit) may have changed because someone merged its work. In order to put your work on top of the last trunk commit you need to rebase your development branch as follows:

`git rebase trunk`

Basically the command checks out the trunk tip and starts applying your branch commits on top of it. If some commit has a conflict, the process will stop and you will need to do a manual merge (basically selecting the changes you want to keep). Once you have decided which changes should be kept add them `git add file_with_changes` and continue the rebase `git rebase --continue`.

Once the process is finished you are ready to finally push your feature branch to the remote with:

`git push origin name_of_the_feature_you_are_developing`


### 5. Open a Merge Request

Trunk branch does not accept pushes to it. To merge the changes you need to issue a Merge Request (MR) using gitlab interface. Go to:

http://compss.bsc.es/gitlab/compss/framework/branches

Your branch should be 0 commits behind trunk (if you did the rebase correctly) and some commits ahead (your commits). Select Merge Request to create one.
Ideally you should assign the MR to someone so they can check that your changes and feature makes sense. You can add some more information for the reviewer as well as some tags if you use them.

### 6. Merge changes

This MR will trigger a runner which will build the trunk you just committed and run the local suite of basic tests (and add the headers in case you forgot to). **Unless all tests are passed, you will not be able to merge your changes.** If you want, you can select 'Merge when pipeline succeeds' and the branch will automatically be merged once testing finishes. 

If you selected the corresponding box, the remote branch will be deleted after the merge. However, your local branch will not be removed. To do so, first change to another branch (like trunk for example with `git co trunk`) and then issue:

`git br -d branch_to_remove`

### 7. Pipeline failed

If you are here your MR tests failed. Fix your branch and just push the changes again. If the MR is still open, the push will cause the tests to run again. Please, run the tests locally first instead of playing trial-and-error with the gitlab runner or you will collapse it. 

**Important** if gitlab runner had to add some headers for you, you may get an error when pushing, telling you that your branch is behind its remote counterpart. This just means that the runner had to commit the headers to the branch and you need to pull that changes to your local history. Issue:

`git pull origin your_branch`

and then just push again

`git push origin your_branch`

To start next feature go to 1.b






