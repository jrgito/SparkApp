# Spider Info

Project dedicated to the data migration of the Datamart of Banco Bilbao Vizcaya. This includes the ingestion of the tables of data that exist in origin, the duplication of the processes on this data as well as the special casuistica of some of them not yet implemented so far.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.
Check each of the modules that come out in this project to perform this operation separately.
  
 
### Prerequisites

* Git
* Maven

### Installing

clone repo

```
git clone git@github.com:DatioBD/spider-info.git
```

create ingest jar

```
cd Ingest
mvn clean install package
```

create PostIngest jar

```
cd PostIngest
mvn clean install package
```

jars should be located in target of the respective directories 

## Running the tests

You can check what is being tested in this route: https://github.com/DatioBD/spider-info/blob/OS-227/Ingest/src/test/scala/com/datiobd/spider/DeleteDuplicatesTest.scala. I have written the tests in Gherkin language, to make it easy to read.

To consult the result the only way to do it now is to download the repository and running it in local, the steps to get it would be:

- Clone the repository https://github.com/DatioBD/spider-info
- Check that you are in the development o master branch.
- Run the test command with maven: mvn clean test


## Deployment

Deployment in Datio platform. Follow this [tutorial](https://datiobd.atlassian.net/wiki/display/PC/Manual+de+usuario)

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **[Jorge Magallón](mailto:jmagallon@datiobd.com)** - *Developer*
* **[Esther Rodríguez](mailto:erodriguez@datiobd.com)** - *Developer*
* **[Monica Aguilar](mailto:maguilar@datiobd.com)** - *Developer*
* **[Jorge Barroso](mailto:jbarroso@datiobd.com)** - *QA*

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc

