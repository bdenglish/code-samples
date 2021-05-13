### fast-inference-server

This was a POC to see what kind of performance I could get out of a Python REST API for inferencing a basic torch model.
We have a fairly tight constraint in our production inferencing where responses need to be returned in ~50ms. This was an
attempt to see if that kind of performance could be achieved in Python (this example usually took about 10-15ms). There
was some lore from before I started that this same concept was attempted using Flask but the response times were way to
high. This was to prove it could be done in python.