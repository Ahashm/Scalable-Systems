var express = require("express");
var router = express.Router();
var dialogflowfulfillment = require("dialogflow-fulfillment");
const { text } = require("express");

const intentsArray = ["Intent 1 - Which repository has the most lines of code?",
  "Intent 2 - At what hour of the day are people committing the most in this repository?",
  "Intent 3 - How many recent contributors in the past 30 days?",
  "Intent 4 - Who has made the latest commit in this repository?",
  "TestDemo"];

/* GET users listing. */
router.post("/", express.json(), function (req, res) {
  let repository = req.body.queryResult.parameters;
  let question = req.body.queryResult.queryText;
  const agent = new dialogflowfulfillment.WebhookClient({
    request: req,
    response: res,
  });

  var intentMap = new Map();

intentsArray.forEach(element => {
  intentMap.set(element, test)
});

function test(agent) {
  try {
    //agent.add(req.body);
    console.log(req.body); //Get all data 
    console.log("Repo: " + JSON.stringify(repository));
    console.log("question: " + question);
    let test = `Hello there, your question is ${question}`;
    res.send(createTextResponse(test))
  } catch (error){
    console.error(error);
  }
}

agent.handleRequest(intentMap);
});

function createTextResponse(textResponse) {
  let response = {
    "fulfillmentText": textResponse,
    "fulfillmentMessages": [
      {
        "text": {
          "text": [
            textResponse
          ]
        }
      }
    ],
    "source": "example.com",
    "payload": {
      "google": {
        "expectUserResponse": true,
        "richResponse": {
          "items": [
            {
              "simpleResponse": {
                "textToSpeech": "this is a simple response"
              }
            }
          ]
        }
      },
      "facebook": {
        "text": "Hello, Facebook!"
      },
      "slack": {
        "text": "This is a text response for Slack."
      }
    }
  }
  return response;
}

/* GET home page. */
router.get("/", function (req, res, next) {
  res.render("index", { title: "Express" });
});

module.exports = router;

