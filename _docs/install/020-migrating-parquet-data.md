---
title: "Migrating Parquet Data"
parent: "Install Drill"
--- 

 [Migrating Parquet data]({{site.baseurl}}/docs/migrating-parquet-data/#how-to-migrate-data) that you generated using Drill 1.2 or earlier is mandatory before using the data in Drill 1.3. The data must be marked as Drill-generated. 

{% include startimportant.html %} Run the upgrade tool only on Drill-generated Parquet files. {% include endimportant.html %}

<!-- as described in [DRILL-4070](https://issues.apache.org/jira/browse/DRILL-4070).  -->

## Why Migrate Drill Data
Drill 1.3 uses the latest Apache Parquet Library when generating and partitioning Parquet files, whereas Drill 1.2 and earlier uses a version of the previous Parquet Library created by the Drill team. The Drill team fixed a bug in the previous Library to accurately process Parquet files generated by other tools, such as Impala and Hive. Apache Parquet fixed the bug in the latest Library, making it suitable for use in Drill 1.3. Drill now uses the same Apache Parquet Library as Impala, Hive, and other software. You need to run the upgrade tool on Parquet files generated by Drill 1.2 and earlier that used the previous Library. 

The upgrade tool simply inserts a version number in the metadata to mark the file as a Drill file. 

<!-- The bug fix eliminated the risk of inaccurate metadata that could cause incorrect results when querying Hive- and Pig-generated Parquet files. No such risk exists with Drill-generated Parquet files. Querying Drill-generated Parquet files, regardless of the Drill version, yields accurate results. Drill-generated Parquet files, regardless of the Drill release, contain accurate metadata. -->


## Preparing for the Migration
Set aside sufficient time for the migration. In a test by Drill developers, it took 32 minutes to upgrade 1TB data in 840 files and 370 minutes to upgrade 100 GB data in 200k files. Although the size of files is a factor in the upgrade time, the number of files is the most significant factor.

System administrators can write a shell script to run the upgrade tool simultaneously on multiple sub-directories.

Back up the data to be migrated and create one or more `temp` directories as described in the next section before migrating the data.

## How to Migrate Data
The `temp` directory or directories hold a copy for recovery of the file(s) currently being modified in the event of a system failure. Inspecting the `temp` directory can also indicate the success or failure of an unattended migration.

To migrate Parquet data for use in Drill 1.3 that you generated in Drill 1.2 or earlier, follow these steps:

{% include startimportant.html %} Run the upgrade tool only on Drill-generated Parquet files. {% include endimportant.html %}

1. Back up the data to be migrated.  
2. Create one or more `temp` directories, depending on how you plan to run the upgrade tool, on the same file system as the data.  
   For example, if the data is on HDFS, create the temp directory on HDFS.
   Create distinct temp directories when you run the upgrade tool simultaneously on multiple directories as different directories can have files with same names.  
3. Download the upgrade tool from [github](https://github.com/parthchandra/drill-upgrade).  
4. If you use [Parquet metadata caching]({{site.baseurl}}/docs/optimizing-parquet-metadata-reading/#how-to-trigger-generation-of-the-parquet-metadata-cache-file):  
   * Delete the cache file you generated from all directories and subdirectories where you plan to run the upgrade tool.  
   * Run REFRESH TABLE METADATA on all the folders where a cache file previously existed.  
5. Run the upgrade tool as shown in the following example:    

             java -Dlog.path=/<your path>/drill-upgrade/upgrade.log -cp drill-upgrade-1.0-jar-with-dependencies.jar org.apache.drill.upgrade.Upgrade_12_13 --tempDir=maprfs:///drill/upgrade-temp maprfs:///drill/testdata/

## Checking the Success of the Migration
If you perform an unattended migration, check that the temp directory or directories are empty. Empty directories indicate success.

## Handling of Migration Failure

If a network connection goes down, or if a user cancels the operation, the file that was being processed at the time of cancellation could be corrupted. To recover from such a situation, perform the following steps:

1. Copy the file back from the temp directory to your directory of Parquet files. 
2. Re-run the upgrade tool.

The tool skips the files that it has already processed and only updates the remaining files.


