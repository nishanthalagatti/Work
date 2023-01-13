const fs = require("fs");
const { parse } = require("csv-parse");

const fetchMatchData = (filePath) => {
  let matchData = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ delimiter: ",", from_line: 2 }))
      .on("data", function (row) {
        let dataObj = {};
        dataObj.id = parseInt(row[0]);
        dataObj.year = parseInt(row[2].substring(0, 4));
        dataObj.team1 = row[6];
        dataObj.team2 = row[7];
        dataObj.winner = row[10];
        matchData.push(dataObj);
      })
      .on("end", function () {
        resolve(matchData);
      })
      .on("error", function (error) {
        reject(error.message);
      });
  });
};

const fetchBallByBallData = (filePath) => {
  let ballByBallData = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ delimiter: ",", from_line: 2 }))
      .on("data", function (row) {
        let dataObj = {};
        dataObj.id = parseInt(row[0]);
        dataObj.bowler = row[6];
        dataObj.extra_runs = parseInt(row[8]);
        dataObj.total_runs = parseInt(row[9]);
        dataObj.bowling_team = row[17];
        ballByBallData.push(dataObj);
      })
      .on("end", function () {
        resolve(ballByBallData);
      })
      .on("error", function (error) {
        reject(error.message);
      });
  });
};

const main = async () => {
  let matchData = await fetchMatchData("./Dataset/IPL Matches 2008-2020.csv");
  let ballByBallData = await fetchBallByBallData(
    "./Dataset/IPL Ball-by-Ball 2008-2020.csv"
  );
  console.log(matchData);
  console.log(ballByBallData);
};

main();
