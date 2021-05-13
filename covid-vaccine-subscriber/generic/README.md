```shell script
python generic/generic_ateb_subscriber.py \
        --patients=../inputs/live_test.json \
        --bot_url=file:///opt/app-root/weis_pa.html \
        --output_path=../output/ \
        --log_level=INFO \
        --live
```
#### Arguments
--patients_file: json file with patient info

--bot_url: html file or url with specific ateb bot code, ** if this is a file it must be present at the location where the webdriver is being executed (selenium-grid vs local)


--output_path: the output path to store logs, screenshots

--no_selenium_grid: pass this option to run against a local web driver

--log_level: defaults to INFO but you can change it here

--live: this needs to be passed in order to book appointments, if not passed the bot will not click 'Yes' at the end



```shell script
docker run --rm --net=host --name wegmans_pa_client \
    -v /Users/ben/git/covid-vaccine-subscriber/inputs:/opt/app-root/input \
    -v /Users/ben/git/covid-vaccine-subscriber/output:/opt/app-root/output:rw \
    -e TZ=America/New_York \
    generic-vaccine-subscriber:latest \
    --patients_file=input/erie.json --output_path=output/wegmans_pa/ --bot_url=file:///opt/app-root/wegmans_pa.html
```