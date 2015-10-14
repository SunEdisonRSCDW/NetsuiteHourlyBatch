var kmsCrypto = require('./kmsCrypto');

var testSimple = function(callback) {
	console.log("Simple Two Way String Encrypt/Decrypt");
	kmsCrypto.encrypt("Testing KMS Crypto", function(err, encryptedCiphertext) {
		if (err) {
			console.log(err);
		} else {
			console.log("Got Encrypted Value " + kmsCrypto.toLambdaStringFormat(encryptedCiphertext));
			
			kmsCrypto.decrypt(encryptedCiphertext, function(err, plaintext) {
				if (err) {
					console.log(err);
				} else {
					console.log("Simple Two Way Encryption Result: " + plaintext.toString());

					if (callback) {
						callback();
					}
				}
			});
		}
	});
};

// blocking array based encryption
var testArray = function(callback) {
	console.log("Test Blocking Array Decryption");

	var plaintext = [ "value 1", "value 2" ];

	// blocking encryption
	kmsCrypto.encryptAll(plaintext, function(err, encryptedArray) {
		// blocking decryption
		console.log("Blocking Decrypt of " + encryptedArray.length + " values");
		kmsCrypto.decryptAll(encryptedArray, function(err, decrypted) {
			if (err) {
				console.log(err);
			} else {
				for (var i = 0; i < decrypted.length; i++) {
					if (decrypted[i]) {
						console.log("Blocking Decryption Value " + i + ": " + decrypted[i].toString());
					} else {
						console.log("Value " + i + " returned undefined");
					}
				}
			}
		});

	});

	// invoke the callback if needed
	if (callback) {
		callback();
	}
};

var testSeralisation = function(callback) {
	console.log("Serialised Two Way Encrypt/Decrypt");
	kmsCrypto.encrypt("Testing KMS Crypto", function(err, encryptedCiphertext) {
		if (err) {
			console.log(err);
		} else {
			// turn the encrypted Ciphertext into a String
			var stringValue = JSON.stringify(encryptedCiphertext);

			// turn the Ciphertext back into a buffer
			var ciphertextBuffer = new Buffer(JSON.parse(stringValue));

			kmsCrypto.decrypt(ciphertextBuffer, function(err, plaintext) {
				if (err) {
					console.log(err);
				} else {
					console.log("Simple Two Way Encryption Result: " + plaintext.toString());

					if (callback) {
						callback();
					}
				}
			});
		}
	});
};

// run the tests
//testSimple(testArray(testSeralisation()));
testSimple(function(){});