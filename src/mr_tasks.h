#pragma once

#include <string>
#include <iostream>
#include <map>
#include <utility>
#include <vector>
#include <fstream>
#include<bits/stdc++.h>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		bool Done();

		std::vector<std::shared_ptr<std::ofstream>> outputFiles;

		int          WorkerID;
    std::string  OutputDirectory;
    int          NumberOfWorkers;
    int          NumberOfFiles;

		std::vector<std::pair<std::string, std::string>> mappedKeyValuePairs;

		void SetWorkerID(int i) { WorkerID = i; }
    void SetOutputDirectory(std::string s) { OutputDirectory = s; }
    void SetNumberOfWorkers(int i) { NumberOfWorkers = i; }
    void SetNumberOfFiles(int i) { NumberOfFiles = i; }
		bool Setup();

		bool WriteShardToIntermediateFile();
		bool DiscardShardResults();
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal()
{
		WorkerID = 0;
    OutputDirectory = "";
    NumberOfWorkers = 0;
    NumberOfFiles = 0;
}

inline bool BaseMapperInternal::Setup()
{
		std::string intermediateOutputFile = OutputDirectory + "/mapper_" + std::to_string(WorkerID);

		for(int i = 0; i < NumberOfFiles; i++)
		{
				std::shared_ptr<std::ofstream> outFile(new std::ofstream(intermediateOutputFile + "_" + std::to_string(i) + ".map"));

				if (!outFile->is_open())
				{
					std::cout << "Could not open output file for writing!\n";
					return false;
				}

				outputFiles.push_back(outFile);
		}

		return true;
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val)
{
		//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;

		mappedKeyValuePairs.push_back(make_pair(key, val));
}



inline bool BaseMapperInternal::WriteShardToIntermediateFile()
{
	  std::cout << "Base mapper writing to intermediate files.";

		for (int i = 0; i < mappedKeyValuePairs.size(); i++)
		{
				std::string key =  mappedKeyValuePairs[i].first;
				std::string value = mappedKeyValuePairs[i].second;

				int whichFile = (std::hash<std::string>{}(key)) % NumberOfFiles;

			  *(outputFiles[whichFile]) << key << " " << value << "\n";
		}

		return true;
}


inline bool BaseMapperInternal::DiscardShardResults()
{
	  mappedKeyValuePairs.clear();
		return true;
}





/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		int          WorkerID;
    std::string  OutputDirectory;
    int          NumberOfWorkers;
    int          NumberOfFiles;
		int          ReduceSubset;

		std::vector<std::ifstream> inputFiles;
		std::ofstream outputFile;

		void SetWorkerID(int i) { WorkerID = i; }
    void SetOutputDirectory(std::string s) { OutputDirectory = s; }
    void SetNumberOfWorkers(int i) { NumberOfWorkers = i; }
    void SetNumberOfFiles(int i) { NumberOfFiles = i; }
    void SetReduceSubset(int i) { ReduceSubset = i; }
		void Setup();


};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
		WorkerID = 0;
		OutputDirectory = "";
		NumberOfWorkers = 0;
		NumberOfFiles = 0;
		ReduceSubset = 0;
}

inline void BaseReducerInternal::Setup()
{
		std::cout << "still have to do this later.\n";
}



/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
