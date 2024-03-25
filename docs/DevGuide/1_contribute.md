# Contribute

In this document the general rules and guidelines for contributing to the **multimno** repository are detailed.

## Source control strategy

This repository uses three principal branches for source control:

* **main:** Branch where the official releases are tagged. The HEAD of the branch corresponds to the 
  latest release of the software.  
* **integration:** Branch used for preproduction testing and validation from which a release to the main branch
will be generated.
* **development:** Branch that centralizes the latest features developed in the repository. After enough features/bugs
have been delivered to this branch, a snapshot will be created in the **integration** branch for testing before 
generating a release.

These three branches shall only accept changes by the repository administrator. Commits shall not be performed directly
in these branches except for small hotfixes in the integration branch.

All features and bug fixes will be developed in branches that origin from the development branch.

## Forking the repository

Developers that want to contribute to the multimno repository shall fork the repository with all its branches. This can 
be done through the github website. After creating a fork of the repository, developers can clone the forked repo in 
their computers.

## Create an Issue with the development that will be performed

First of all, Check if the issue you will develop already exists. 
Then, create an issue in the multimno repository stating the objective of the development that will be performed. Templates for creating issues for features or fixes are provided in the repository.


## Creating a feature/fix branch

Within the forked repository developers shall create a branch that originates from the **development** branch. 
This branch shall have the following naming convention:

* feat_\<name\>: If it is a new feature.
* fix_\<name\>: If it is a bug solution.

Please remember to keep the forked development branch up-to-date with the latest changes.

Don't forget to look up the [developer guide](./2_dev_guidelines.md) to check the code style, testing and development
practices that shall be followed to develop new code for the multimno repository.

## Opening a pull request

Before opening a pull request in the multimno repository verify:
* Latest development changes are merged into the branch that performs the pull request. 
* All tests pass successfully.
* All the new code is documented following the Google style docstrings.
* New tests for the code developed are included and pass successfully.

Use the github web to create a pull request. The pull request must deliver your developed branch to the **development**
branch of the multimno repository. Associate the PR(pull request) to the previously created issue.

## The review process & pull request closure

The repository administrators will review the pull request performed to the development branch. 

* If the changes are accepted, they will be incorporated in the development branch and the pull request will be closed. 
* If the changes are not accepted, the repository administrators may indicate as a comment in the pull request feedback 
and modifications needed to accept the pull request. However, the pull request may be desestimated to which it 
will be closed and changes will not be incorporated.