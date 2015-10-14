var upgrade = require('./upgrades');
var region = process.env['AWS_REGION'];
var aws = require('aws-sdk');
aws.config.update({
	region : region
});
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : region
});
var common = require('./common');

exports.test = function() {
	// internal function used to test encapsulation of the callback for the
	// upgrade completion
	var outcome = function(err, s3Info, config) {
		if (config.version === '2.0.0') {
			console.log('OK');
		} else {
			console.log('Upgrade Failed');
		}
		console.log(JSON.stringify(config));
	};

	// generate a sample prefix
	var inputInfo = {
		bucket : 'lambda_redshift_loader_test',
		key : 'input/test1',
		prefix : 'lambda-redshift-loader-test/input/test1',
		inputFilename : 'file.csv'
	};

	// go get the prefix so we have a real item to test with
	var dynamoLookup = {
		Key : {
			s3Prefix : {
				S : inputInfo.prefix
			}
		},
		TableName : configTable,
		ConsistentRead : true
	};
	dynamoDB.getItem(dynamoLookup, function(err, data) {
		upgrade.upgradeAll(dynamoDB, inputInfo, data.Item, outcome);
	});
};

exports.test();