//sample data
//1043224093,FLT141,-34.98806,138.57323,0,0,2018-01-01 11:06:24.0000000 +10:30,2018-01-01 11:06:32.8295340 +10:30,116
//1043528837,FLT264,-34.96933,138.5283,21,19,2018-01-01 13:59:45.0000000 +10:30,2018-01-01 13:59:47.1264476 +10:30,178
//1043528848,FLT274,-34.32647,138.78715,60,52,2018-01-01 13:59:46.0000000 +10:30,2018-01-01 13:59:47.1264476 +10:30,446
//1043528882,FLT350,-32.53472,137.84415,70,305,2018-01-01 13:59:45.0000000 +10:30,2018-01-01 13:59:49.1076984 +10:30,410
//1043528889,FLT284,-34.19315,140.28564,36,31,2018-01-01 13:59:45.0000000 +10:30,2018-01-01 13:59:49.1232988 +10:30,490


var vehicles = [];
var stats = {};
	stats.processed = 0;
	stats.outputted = 0;
	stats.started;
	
var outputFile = process.argv[3];
var inputFile = process.argv[2];
var significantDistance = process.argv[4];


stats.started = Date.now();

var fs = require('fs');
var LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader(inputFile);



function getDistance(lat1, lon1, lat2, lon2) {
  var R = 6371; 
  var dLat = deg2rad(lat2 - lat1);
  var dLon = deg2rad(lon2 - lon1);
  var a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  var d = R * c; 
  return d;
}

function deg2rad(deg) {
  return deg * (Math.PI / 180)
}



function getVehicle(fleet){
	return  vehicles.find(function( obj ) { return obj.name === fleet; });
}



function pushPosition(record){
	stats.outputted++;
	record.join(",")
	fs.appendFile(outputFile, record.join(",") + '\r\n', function (err) {})
}


function createVehicle(record){
	var vehicle = { name: record[1], lat:record[2], lng: record[3] };
	vehicles.push(vehicle);
	pushPosition(record);
	return vehicle;
}

function processPosition(record, fleet){
	stats.processed++;
	if (getDistance(record[2], record[3], fleet.lat, fleet.lng) > significantDistance){
		pushPosition(record);
	} 
	fleet.lat = record[2];
	fleet.lng = record[3];
}



function processRecord(line){
	var record = line.split(',');
	fleet = getVehicle(record[1]);	
	if (!fleet){
		fleet = createVehicle(record);
	}
	
	processPosition(record, fleet);
}


lr.on('error', function (err) {
});

lr.on('line', function (line) {
	processRecord(line);
});

lr.on('end', function () {
	console.log('Processing time (s):' + ((Date.now()-stats.started) / 1000));
	console.log('Number of records in input file:' + stats.processed);
	console.log('Total records retained:' + stats.outputted);
	console.log('Percentage retained:' + Number.parseFloat(stats.outputted/stats.processed * 100).toFixed(2));
});
