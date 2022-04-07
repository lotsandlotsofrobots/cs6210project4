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

		void SetIntermediateOutputFile(std::string s) { intermediateOutputFile = s; }
		std::string GetIntermediateOutputFile() { return intermediateOutputFile; }

		std::vector< std::pair<std::string, std::string> > mappedKeyValuePairs;

		std::string intermediateOutputFile;
		int numOutputFiles;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
		intermediateOutputFile = "";
		numOutputFiles = 0;
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val)
{
		std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;

		mappedKeyValuePairs.push_back(make_pair(key, val));
}

inline bool BaseMapperInternal::Done()
{
	  std::cout << "Base mapper DONE, writing to intermediate file.";

		// have to use a shared ptr here because ofstream doesn't allow copy constructor
		std::vector<std::shared_ptr<std::ofstream>> outputFiles;

		for(int i = 0; i < numOutputFiles; i++)
		{
			  std::shared_ptr<std::ofstream> outFile(new std::ofstream(intermediateOutputFile + "_" + std::to_string(i) + ".map"));

				if (!outFile->is_open())
				{
					std::cout << "Could not open output file for writing!\n";
					return false;
				}

			  outputFiles.push_back(outFile);
		}

		sort(mappedKeyValuePairs.begin(), mappedKeyValuePairs.end());

		// HASH KEYS TO DIFFERENT FILES !!!
		// if n input files, dump to hash(key) % n

		for (int i = 0; i < mappedKeyValuePairs.size(); i++)
		{
				std::string key =  mappedKeyValuePairs[i].first;
				std::string value = mappedKeyValuePairs[i].second;

				int whichFile = (std::hash<std::string>{}(key)) % numOutputFiles;

			  *(outputFiles[whichFile]) << key << " " << value << "\n";
		}


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
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
