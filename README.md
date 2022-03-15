# Couchbase Load Testing using NBomber

This repo is a .NET 6 console application that uses the popular .NET load testing framework NBomber to test the performance of inserting documents into a Couchbase Bucket.  

For an overview of NBomber - please checkout there website:
[NBomber Overview](https://nbomber.com/docs/overview)

## Build
- clone repo
- cd into repo directory
- cd src
- cd cb-dotnet-load-test
- dotnet restore
- dotnet build
- dotnet run

Note that you can tweak all the values for the Scenerio.  See NBomber [documenation](https://nbomber.com/docs/general-concepts#scenario) to learn more about this, but generally you should update:

- RampConstant
- KeepConstant
- InjectPerSecond
- InjectPerSecondRandom

This will automatically delete the bucket and then created it with 1024 MB of RAM dedicated to the bucket.  You can change the settings in the CreateBucket method to fit your needs.

**This repo is provided AS-IS with no support.**
