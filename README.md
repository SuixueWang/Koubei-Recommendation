# Koubei-Recommendation

Introduction 
-----------------------------
    
    This Project is about the competition of Brick-and-Mortar Store Recommendation with Budget Constraints.

Directory Structure.
-----------------------------

	└── README.md                           # deacription

	# create table and load csv datasets into MySQL database.
	├── LoadAndQuery.sql


	# processing functions
	├── Main_getDataSet_csv.py              # extracting features and getting datasets. 
	├── Main_model.py                       # main fuction
	├── mymodel.py                          # functions like training, testing, and selecting for Xgboost datasets
	├── mymodel_csv.py                      # functions like training, testing, and selecting for csv datasets
	├── Main_saveDMatrix.py                 # saving the datasets to specific format that fitting Xgboost classififer.
	├── Model_cross_validation.py           # evaluating offline datasets by cross-validation.

	# subdirectories
	├── FeatureSelect                       # extracting detail features, such as Item Feature, User Feature and Union Feature.
	├── pd_csv_get_chunk                    # tool for reading csv files fastly and under sampling. 
