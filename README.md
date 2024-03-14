magda-cli-py
============

Python client for magda REST API

A simple REST API client build on top of a generic APIClient that takes care of the encoding and packaging of parameters.

# Usage

Create a singleton client
$ from magdacli.magda import createRegistryClient
$ client = createRegistryClient({"api-key":"<API-KEY>,"api-key-id":"<API-KEY-ID>","url":"<BASE-URL>"})


Create an instance
$ from magdacli.magda import AspectMagdaClient
$ client = AspectMagdaClient({"api-key":"<API-KEY>,"api-key-id":"<API-KEY-ID>","url":"<BASE-URL>"})
	
	
Dependencies
============

* restpy

License
=======

Short version: [MIT](https://en.wikipedia.org/wiki/MIT_License)
Long version: see [LICENSE.txt](LICENSE.txt) file
	 