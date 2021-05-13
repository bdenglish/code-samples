### covid-vaccine-subscriber

this was a project I worked on in my free time to help friends and relatives and eventually strangers find covid vaccines. 
It is entirely written in python and has some `data-engineering` components. The goal here was to iterate quickly and optmize
only what needed to be optimized. I mainly included this project because I worked on it recently so I can speak about it in detail.

* geocode/download_patients.py is a basic etl type script to download patient details from a google sheet (where people had signed up through a google form)
and identify pharmacies within a certain distance and then output a configuration file that the bot would read from
  
* generic/generic_ateb_subscriber.py - is a python application that uses the selenium api to control a web browser and identify
available appointments and then book them if the details match any of the patients' parameters.
  
Prior to this project I had no experience with Selenium. The project run inside a docker container mostly because that
is my default but it allowed for easy parallelization (docker-compose).