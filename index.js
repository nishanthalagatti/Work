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

const query1 = (matchData) => {
  let ans = {};
  for (let match of matchData) {
    if (ans.hasOwnProperty(match.year)) ans[match.year]++;
    else ans[match.year] = 1;
  }
  console.log(ans);
};

const query2 = (matchData) => {
  let ans = {};
  for (let match of matchData) {
    if (ans.hasOwnProperty(match.year)) {
      if (ans[match.year].hasOwnProperty(match.winner)) {
        ans[match.year][match.winner]++;
      } else {
        ans[match.year][match.winner] = 1;
      }
    } else {
      ans[match.year] = {};
      ans[match.year][match.winner] = 1;
    }
  }
  console.log(ans);
};

const query3 = (matchData, ballByBallData, year) => {
  const ans = {};
  const matchesPlayedInYear = matchData.filter((match) => match.year === year);
  const matchIds = matchesPlayedInYear.map((match) => match.id);
  const reqBallByBallData = ballByBallData.filter(
    (ball) => matchIds.indexOf(ball.id) !== -1
  );
  for (let ball of reqBallByBallData) {
    if (ans.hasOwnProperty(ball.bowling_team))
      ans[ball.bowling_team] += ball.extra_runs;
    else ans[ball.bowling_team] = ball.extra_runs;
  }
  console.log(ans);
};

const query4 = (matchData, ballByBallData, year) => {
  const stats = {};
  const matchesPlayedInYear = matchData.filter((match) => match.year === year);
  const matchIds = matchesPlayedInYear.map((match) => match.id);
  const reqBallByBallData = ballByBallData.filter(
    (ball) => matchIds.indexOf(ball.id) !== -1
  );
  for (let ball of reqBallByBallData) {
    if (stats.hasOwnProperty(ball.bowler)) {
      stats[ball.bowler].runs += ball.total_runs;
      stats[ball.bowler].balls++;
    } else {
      stats[ball.bowler] = {};
      stats[ball.bowler].runs = ball.total_runs;
      stats[ball.bowler].balls = 1;
    }
  }

  let bowlerEconomies = [];
  for (let bowler in stats) {
    bowlerEconomies.push({
      bowler: bowler,
      economy: stats[bowler].runs * 6 / stats[bowler].balls,
    });
  }
  bowlerEconomies.sort((a, b) => a.economy - b.economy);
  console.log(bowlerEconomies.slice(0, 10));
};

const main = async () => {
  let matchData = await fetchMatchData("./Dataset/IPL Matches 2008-2020.csv");
  query1(matchData);
  query2(matchData);
  let ballByBallData = await fetchBallByBallData(
    "./Dataset/IPL Ball-by-Ball 2008-2020.csv"
  );
  query3(matchData, ballByBallData, 2016);
  query4(matchData, ballByBallData, 2015);
};

main();
