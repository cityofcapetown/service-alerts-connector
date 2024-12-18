# ⚠️ City of Cape Town Service Alerts ⚠️
This repository contains the specifications and code that support the public service alert data API. 

This API is the product of an initiative by various departments within the City of Cape Town to make available critical
information about the delivery of services that is both timely **and** useful.

---

## Getting Started

### 🔗 Links
Adapted from [this links doc](https://gist.github.com/Gordonei/947ff2ae93b8a7983594244053724161), these are the live 
links to the various alerts:
* [Current Unplanned Outages](https://service-alerts.cct-datascience.xyz/v1.2/service-alerts/current/unplanned)
* [Current Planned outages](https://service-alerts.cct-datascience.xyz/v1.2/service-alerts/current/planned)
* [Unplanned outages from the last 7 days](https://service-alerts.cct-datascience.xyz/v1.2/service-alerts/7days/unplanned)
* [Planned outages from the last 7 days](https://service-alerts.cct-datascience.xyz/v1.2/service-alerts/7days/planned)
* [All unplanned outages](https://service-alerts.cct-datascience.xyz/v1.2/service-alerts/all/unplanned)
* [All planned outages](https://service-alerts.cct-datascience.xyz/v1.2/service-alerts/all/planned)

### 🏗️ Generating Client Libraries from the API Specification

Included in this repo is [an OpenAPI specification for the data available](./service-alerts-api.yaml). This specification
can be used to generate a client library in the language of your choice, using tools such as the [OpenAPI Generator](https://github.com/OpenAPITools/openapi-generator).

#### Python Example

Here is how you would do it for python, using the `openapitools/openapi-generator-cli` Docker image:

1. Clone this repo, and change to it.
2. Generate the API using the docker image (The client library will be generated in the `/dist` directory within this repo.):
    ```bash
    docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
        -i /local/service-alerts-api.yaml \
        -g python \
        --package-name cct_service_alerts \
        -o /local/dist/python
    ```
3. Install the dependencies for the new client library: `pip3 install -r ./dist/python/requirements.txt`
4. Give the new client library a spin using [this simple example script](./bin/api_example_script.py): `PYTHONPATH=./dist/python python3 bin/api_example_script.py`:
```
$ PYTHONPATH=./dist/python python3 bin/api_example_script.py
[ServiceAlertV1(id=23121, service_area='Refuse', title='Refuse delays', description='Please leave bin out until 21:00\nIf not serviced\ntake bin onto property and place out by 06:30 the following day', area='Area Central Collections', location='Backlog Suburbs in Progress: Delayed Suburbs: Wednesday, 14 February 2024 Comment\n\uf0b7 None\n\uf0b7 De La Haye\n\uf0b7 Belgravia\n\uf0b7 Chrismar\n\uf0b7 Loumar\n\uf0b7 Flats in Blomtuin\n\uf0b7 Leonsdale\n\uf0b7 Welgelegen\n\uf0b7 Kleinbosch\n\uf0b7 Ravensmead\n\uf0b7 Leonsdale\n\uf0b7 Adriaanse\n\uf0b7 Clarkes Est\n\uf0b7 Uitsig\n\uf0b7 The Range\n\uf0b7 Cravenby\n\uf0b7 Bishop Lavis\n\uf0b7 Nooitgedaacht\n\uf0b7 Avonwood\n\uf0b7 Balvenie\n\uf0b7 Florida\n\uf0b7 Connaught\n\uf0b7 Platterkloof 1\n\uf0b7 Avon\n\uf0b7 Elsies River\n\uf0b7 Avon\n\uf0b7 Elsies River\n\uf0b7 delft 7 & 8\n\uf0b7 Delft South\n\uf0b7 Leiden\n\uf0b7 Crawford', publish_date='2024-02-13T22:00:00.000Z', effective_date='2024-02-13T22:00:00.000Z', expiry_date='2024-02-15T22:00:00.000Z', start_timestamp='2024-02-14T04:00:00.000Z', forecast_end_timestamp='2024-02-15T19:00:00.000Z', planned=False, request_number=None, tweet_text=None, toot_text=' 🚮 Refuse Delays 🚮\n📍Area Central Collections\n⏰Feb 14, 6:00 AM - Feb 15, 9:00 PM\nExpect delays in refuse collection. If not serviced, leave bin out until 9 PM. If still not collected, take bin onto property & place out by 6:30 AM the next day.\n#CapeTown #Refuse'),
 ServiceAlertV1(id=23118, service_area='Refuse', title='Refuse delays', description='Please leave bin out until 21:00\nIf not serviced\ntake bin onto property and place out by 06:30 the following day', area='Area South Collections', location='Backlog Suburbs in Progress: Delayed Suburbs: Wednesday, 14 February 2024 Comment\n\uf0b7 Lmhoffs Gift\n\uf0b7 Blue Water Estate\n\uf0b7 Kommetjie\n\uf0b7 Glencairn Heights\n\uf0b7 Grassy Park\n\uf0b7 Zeekoevlei\n\uf0b7 Rocklands\n\uf0b7 Oceanview\n\n\n\n', publish_date='2024-02-13T22:00:00.000Z', effective_date='2024-02-13T22:00:00.000Z', expiry_date='2024-02-15T22:00:00.000Z', start_timestamp='2024-02-14T04:00:00.000Z', forecast_end_timestamp='2024-02-15T19:00:00.000Z', planned=False, request_number=None, tweet_text=' 🚮 Refuse Delays 🚮\n📍Area South Collections\n⏰Feb 14, 6:00 AM - Feb 15, 9:00 PM\nDue to backlog, please leave bin out until 9 PM if not serviced. If still not collected by 6:30 AM next day, take bin onto property & place out again.', toot_text=' 🚮 Refuse Delays 🚮\n📍Area South Collections\n⏰Feb 14, 6:00 AM - Feb 15, 9:00 PM\nDue to backlog, refuse collection may be delayed in these areas. Please leave bin out until 9 PM. If not serviced, take bin onto property & place out by 6:30 AM next day.\n#CapeTown #Refuse')]
```

### ✉️ PubSub
We also maintain an [AWS SNS](https://aws.amazon.com/sns/) topic: `arn:aws:sns:af-south-1:566800947500:service-alerts`

The topic publishes whenever there are new alert(s) created. The content of the message will be a JSON array containing 
the contents of the alerts.

---

## ❓ FAQ ❓
### What is a Service Alert?
A service alert is a notification of a significant disruption to the delivery of a service provided by the City of Cape
Town. While any loss of service is regrettable, it is hoped that by providing as much information as possible about the
outage, its effect may be mitigated.

This dataset was originally created for internal use by customer relations staff, to assist in answering queries from
residents.

### Who is capturing this information?
The short answer is officials at the City of Cape Town. 

The more detailed answer is that it is usually administrative officials in the technical operations centres that 
coordinate the operations of various essential services such as Water and Sanitation, Energy, etc.

### Why are you making this information publicly available?
There are several reasons:
* Mitigating the effect of service disruptions to residents by sharing information to help them plan.
* Reducing load on the City's Customer Relations functions
* Transparency - as public officials, we are strongly encouraged to operate as openly as possible.

### What do you want people to do with this information?
Make this information available wherever it may be useful, in the best possible form.

### Who is using this information?
* [Unofficial Mastodon Bot](https://botsin.space/@coct_service_alerts) and [its code](https://github.com/cityofcapetown/mastodon-bots)
* [CCT Data Science Twitter Bot](https://twitter.com/DataOpm) and [its code](https://github.com/cityofcapetown/twitter-bots)
* [CCT Website](https://www.capetown.gov.za/Pages/City-Alerts.aspx)
* [EskomSePush](https://esp.info)

(please feel free to submit pull requests with your use cases!)

---

Made with ❤️ by OPM Data Science
