#pragma once

#include <string>
#include <iostream>
#include <map>
#include <utility>
#include <vector>
#include <fstream>
#include <experimental/filesystem>
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

		std::vector<std::ofstream*> outputFiles;
		//std::vector<std::shared_ptr<std::ofstream>> outputFiles;

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
		std::cerr << "Setting up:\n" << "\n";

    if (WorkerID == 0)
		{
				system("rm intermediate/*");
				system("mkdir -p intermediate");
		}

//   	system("rm intermediate/*");
//		system("mkdir -p intermediate");

		std::string intermediateOutputFile = "./intermediate/mapper_" + std::to_string(WorkerID);

		for(int i = 0; i < NumberOfFiles; i++)
		{
				//std::shared_ptr<std::ofstream> outFile(new std::ofstream(intermediateOutputFile + "_" + std::to_string(i) + ".map"));
				std::ofstream * outFile = new std::ofstream(intermediateOutputFile + "_" + std::to_string(i) + ".map");

				if (!outFile->is_open())
				{
				  	std::cerr << "Could not open output file for writing!\n";
				  	return false;
				}

				outputFiles.push_back(outFile);
		}

		return true;
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val)
{
		//std::cerr << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;

		mappedKeyValuePairs.push_back(make_pair(key, val));
}



inline bool BaseMapperInternal::WriteShardToIntermediateFile()
{
	  //std::cerr << "Base mapper writing to intermediate files:";

		for (int i = 0; i < mappedKeyValuePairs.size(); i++)
		{
				std::string key =  mappedKeyValuePairs[i].first;
				std::string value = mappedKeyValuePairs[i].second;

				int whichFile = (std::hash<std::string>{}(key)) % NumberOfFiles;

				std::ofstream * f = outputFiles[whichFile];

			  *f << key << " " << value << "\n";
		}

		for(int i = 0; i < outputFiles.size(); i++)
		{
			  std::ofstream * f = outputFiles[i];
			  f->flush();
		}

		mappedKeyValuePairs.clear();

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

		int          desiredShardSize;
		int          numberOfShardsTotal;
		std::string  inputFiles;

		std::ofstream outputFile;

		std::map<std::string, std::string> reduceKeyValuePairs;

		void SetWorkerID(int i) { WorkerID = i; }
    void SetOutputDirectory(std::string s) { OutputDirectory = s; }
    void SetNumberOfWorkers(int i) { NumberOfWorkers = i; }
    void SetNumberOfFiles(int i) { NumberOfFiles = i; }
    void SetReduceSubset(int i) { ReduceSubset = i; }
		void Setup();

		void WriteReduce(int reduceID);
		void DiscardReduce();

		void SetdesiredShardSize(int i) { desiredShardSize = i; }
		void SetnumberOfShardsTotal(int i) { numberOfShardsTotal = i; }
		void SetInputFiles(std::string s) { inputFiles = s; }

		void DebugMD5print();
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
		WorkerID = 0;
		OutputDirectory = "unassigned";
		NumberOfWorkers = 0;
		NumberOfFiles = 0;
		ReduceSubset = 0;
		inputFiles = "unassigned";
}

inline void BaseReducerInternal::Setup()
{
		std::cerr << "Setting up:\n" << "\n";

		std::string rm = "rm " + OutputDirectory + "/*";
		std::string mkdir = "mkdir -p " + OutputDirectory;

		std::cerr << "WorkerID: " << std::to_string(WorkerID) << "\n";
		std::cerr << "Output Dir: " << OutputDirectory << "\n";
		std::cerr << "Number of workers: " << std::to_string(NumberOfWorkers) << "\n";
		std::cerr << "Number of files: " << std::to_string(NumberOfFiles) << "\n";

		std::cerr << "Desired shard size: " << std::to_string(desiredShardSize) << "\n";
		std::cerr << "Number of shards total: " << std::to_string(numberOfShardsTotal) << "\n";
		std::cerr << "Input files: " << this->inputFiles << "\n";

		if (WorkerID == 0)
		{
				system(rm.c_str());
				system(mkdir.c_str());
		}

}



/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {

		/*if ( reduceKeyValuePairs.count(key) == 0 )
		{*/
			  reduceKeyValuePairs[key] = val;
		//}

		//std::cerr << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;

		//std::cerr << "Emit called (" << key << ", " << val << ")\n";
}


inline void BaseReducerInternal::WriteReduce(int reduceID)
{
		std::string filename = OutputDirectory + "/reduce_" + std::to_string(reduceID) + ".out";
		outputFile = std::ofstream(filename);

		std::cerr << "Write reduce called.\n";

		for (std::map<std::string, std::string>::iterator i = reduceKeyValuePairs.begin(); i != reduceKeyValuePairs.end(); i++)
		{
			  outputFile << i->first << " " << i->second << "\n";
		}

		outputFile.flush();

		std::this_thread::sleep_for(std::chrono::milliseconds(10000));






//		system(cmd.c_str());

		reduceKeyValuePairs.clear();
}

inline void BaseReducerInternal::DiscardReduce()
{
	  reduceKeyValuePairs.clear();
}


inline void BaseReducerInternal::DebugMD5print()
{
		// Combine

		std::string cmd1 = "cat ./output/* > ./output/combined.out";
		char buffer1[128];
		std::string result1 = "";
		FILE* pipe1 = popen(cmd1.c_str(), "r");

		if (!pipe1) throw std::runtime_error("popen() failed!");
		try {
				std::cerr << "Results of cat combine:\n";
				while (fgets(buffer1, sizeof buffer1, pipe1) != NULL) {
						result1 += buffer1;
				}
		} catch (...) {
				pclose(pipe1);
				throw;
		}
		pclose(pipe1);

		std::cerr << result1 << "\n";




		// Sort

		std::string cmd2 = "sort ./output/combined.out > ./output/sortedcombined.out";
		char buffer2[128];
		std::string result2 = "";
		FILE* pipe2 = popen(cmd2.c_str(), "r");

		if (!pipe2) throw std::runtime_error("popen() failed!");
		try {
				std::cerr << "Results of sort:\n";

				while (fgets(buffer2, sizeof buffer2, pipe2) != NULL) {
						result2 += buffer2;
				}
		} catch (...) {
				pclose(pipe2);
				throw;
		}
		pclose(pipe2);

		std::cerr << result2 << "\n";




		// md5

		std::string filename = "./output/sortedcombined.out";

		std::string cmd3 = "md5sum " + filename;

		char buffer3[128];
		std::string result3 = "";
		FILE* pipe3 = popen(cmd3.c_str(), "r");

		if (!pipe3) throw std::runtime_error("popen() failed!");
		try {
			std::cerr << "Results of sortcombine:\n";

				while (fgets(buffer3, sizeof buffer3, pipe3) != NULL) {
						result3 += buffer3;
				}
		} catch (...) {
				pclose(pipe3);
				throw;
		}
		pclose(pipe3);

		std::cerr << "MD5SUM of file!  - " << result3 << "\n";
}
