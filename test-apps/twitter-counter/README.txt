An application that displays the current top 10 topics, as gathered from the twitter sample stream.
It was ported and adapted from S4 0.3

Architecture:
- twitter-adapter app in adapter node connects to the twitter stream, extracts the tweeted text and passes that to the application cluster
- twitter-counter app in the application cluster receives the text of the tweets, extracts the topics, counts topic occurences and periodically displays the top 10 topics on the console

How to configure:
- you need a twitter4j.properties file in your home dir, with the following properties filled:
debug=true|false
user=<a twitter user name>
password=<the matching password>

How to run:

Please follow the instructions in the S4 piper walkthrough at the following place:
https://cwiki.apache.org/confluence/display/S4/S4+piper+walkthrough