var express = require("express");
var router = express.Router();
var dialogflowfulfillment = require("dialogflow-fulfillment");
const { text } = require("express");

/*
"Intent 1 - Which repository has the most lines of code?"
"Intent 2 - At what hour of the day are people committing the most in this repository?"
"Intent 3 - How many recent contributors in the past 30 days?"
"Intent 4 - Who has made the latest commit in this repository?"
*/

/* GET users listing. */
router.post("/", express.json(), function (req, res) {
  var params = req.body.queryResult.parameters;
  var question = req.body.queryResult.queryText;
  var intent = req.body.queryResult.intent.displayName;
  var intentMap = new Map();

  const agent = new dialogflowfulfillment.WebhookClient({
    request: req,
    response: res,
  });

  console.log(intent);
  switch (intent) {
    case "Intent 1":
      intentMap.set(intent, intentOneResponse);
      break;
    case "Intent 2":
      intentMap.set(intent, intentOneResponse);
      break;
    case "Intent 3":
      console.log("3")
      break;
    case "Intent 4":
      console.log("4")
      break;
  }

  function intentOneResponse() {
    let response = "";
    switch (intent) {
      case "Intent 1":
        let numb = 1000;
        response = `The repository: ${params.repository}, has ${numb} lines of code.`;
        break;
      case "Intent 2":
        let time = "14:00 - 18:00"
        response = `Users are committing the most in: ${params.repository}, between the time ${time}`;
        break;
      case "Intent 3":
        console.log("3")
        break;
      case "Intent 4":
        console.log("4")
        break;
    }
    try {
      agent.add(response)
    } catch (error) {
      errorMessageHandler(error);
    }
  }

  function errorMessageHandler(error) {
    console.error(error);
    console.log("Error occured: " + error);
  }

  agent.handleRequest(intentMap);
});

//GET home page. 
router.get("/", function (req, res, next) {
  res.render("index", { title: "Express" });
});


module.exports = router;

