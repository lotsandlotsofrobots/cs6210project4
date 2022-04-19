#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <arpa/inet.h>

/*
n_workers=6
worker_ipaddr_ports=localhost:50051,localhost:50052,localhost:50053,localhost:50054,localhost:50055,localhost:50056
input_files=input/testdata_1.txt,input/testdata_2.txt,input/testdata_3.txt
output_dir=output
n_output_files=8
map_kilobytes=500
user_id=cs6210
*/


/* DONECS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
		int												 numberOfWorkers = -1;
		std::vector<std::string>   ipAddressAndPorts;
		std::vector<std::string>   inputFiles;
		std::string 						   outputDir = "null";
		int                        numberOfOutputFiles = 0;
		unsigned long long 				 desiredShardSize = 0;
		std::string                userID = "null";
};


/* DONECS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec)
{
		std::ifstream configFile(config_filename);

		if (!configFile.is_open())
		{
			  std::cerr << "Couldn't open config file \"" << config_filename << "\"\n";
				return false;
		}

		std::string line;
		int lineNumber = 1;

		while ( getline(configFile, line) )
		{
			  std::string key;
				std::string value;

				size_t split = line.find('=');

				if (split == std::string::npos)
				{
					  std::cerr << "Error, invalid config line(" << std::to_string(lineNumber) << "): \"" << line << "\"\n";
				}

				key = line.substr(0, split);
				value = line.substr(split+1, line.length()-1);

				if (key == "n_workers")
				{
					  try
						{
							  mr_spec.numberOfWorkers = stoi(value);
						}
						catch (std::exception &e)
						{
						    std::cerr << "Error, invalid config line(" << std::to_string(lineNumber) << "): \"" << line << "\"\n";
								return false;
						}
				}
				else if (key == "worker_ipaddr_ports")
				{
						size_t split = value.find(',');
						while (split != std::string::npos)
						{
							  std::string ipPort = value.substr(0, split);
								value = value.substr(split+1, value.length());

								mr_spec.ipAddressAndPorts.push_back(ipPort);
								split = value.find(',');
						}

						mr_spec.ipAddressAndPorts.push_back(value);

						for (int i = 0; i < mr_spec.ipAddressAndPorts.size(); i++)
						{
							  std::cerr << "ipPort[" << std::to_string(i) << "] - " << mr_spec.ipAddressAndPorts[i] << "\n";
						}
				}
				else if (key == "input_files")
				{
						size_t split = value.find(',');
						while (split != std::string::npos)
						{
								std::string inputFile = value.substr(0, split);
								value = value.substr(split+1, value.length());

								mr_spec.inputFiles.push_back(inputFile);
								split = value.find(',');
						}

						mr_spec.inputFiles.push_back(value);

						for (int i = 0; i < mr_spec.inputFiles.size(); i++)
						{
								std::cerr << "inputFile[" << std::to_string(i) << "] - " << mr_spec.inputFiles[i] << "\n";
						}
				}
				else if (key == "output_dir")
				{
						mr_spec.outputDir = value;
				}
				else if (key == "n_output_files")
				{
						try
						{
								mr_spec.numberOfOutputFiles = stoi(value);
						}
						catch (std::exception &e)
						{
								std::cerr << "Error, invalid config line(" << std::to_string(lineNumber) << "): \"" << line << "\"\n";
								return false;
						}
				}
				else if (key == "map_kilobytes")
				{
						try
						{
								mr_spec.desiredShardSize = stoi(value);
						}
						catch (std::exception &e)
						{
								std::cerr << "Error, invalid config line(" << std::to_string(lineNumber) << "): \"" << line << "\"\n";
								return false;
						}
				}
				else if (key == "user_id")
				{
						mr_spec.userID = value;
				}
		}

		std::cerr << "Set up Mr. Spec" << "\n";

		return true;
}


/* MOSTLY CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec)
{
		std::string error = "";
		if (mr_spec.numberOfWorkers == -1)
		{
				std::cerr << "numberOfWorkers not configured.\n";
				return false;
		}
		if (mr_spec.ipAddressAndPorts.size() == 0)
		{
				std::cerr << "ipAddressAndPorts not configured.\n";
				return false;
		}
		if (mr_spec.inputFiles.size() == 0)
		{
				std::cerr << "inputFiles not configured.\n";
				return false;
		}
		if (mr_spec.outputDir == "null")
		{
				std::cerr << "outputDir not configured.\n";
				return false;
		}
		if (mr_spec.numberOfOutputFiles == 0)
		{
				std::cerr << "numberOfOutputFiles not configured.\n";
				return false;
		}
		if (mr_spec.desiredShardSize == 0)
		{
				std::cerr << "desiredShardSize not configured.\n";
				return false;
		}
		if (mr_spec.userID == "null")
		{
				std::cerr << "userID not configured.\n";
				return false;
		}

		for(int i = 0; i < mr_spec.ipAddressAndPorts.size(); i++)
		{
				std::string ipOrHostName;
				std::string port;
				std::string value = mr_spec.ipAddressAndPorts[i];

				size_t split = value.find(':');
				if (split == std::string::npos)
				{
					  std::cerr << value << " is not a valid ipAddr:port (missing ':')\n";
						return false;
				}

				ipOrHostName = value.substr(0, split);
				port = value.substr(split+1, value.size());

			  struct sockaddr_in sockaddr;
        int rva = inet_pton(AF_INET, ipOrHostName.c_str(), &(sockaddr.sin_addr));

				if (rva != 0)
				{
						std::cerr << value << " is not a valid ipAddr:port (host invalid)\n";
						return false;
				}

				try
				{
					  int temp = stoi(port);
				}
				catch (std::exception &e)
				{
					  std::cerr << value << " is not a valid ipAddr:port (port invalid)\n";
						return false;
				}
	  }


		std::cerr << "FINISH THIS LATER !!!\n";

		return true;
}
