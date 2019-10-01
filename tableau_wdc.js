(function () {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function (schemaCallback) {
        var timeseries_columns = [{
            id: "timestamp",
            dataType: tableau.dataTypeEnum.datetime
        }, {
            id: "open",
            alias: "open",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "high",
            alias: "high",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "low",
            alias: "low",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "close",
            alias: "close",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "volume",
            alias: "volume",
            dataType: tableau.dataTypeEnum.float
        }];

        var tableSchema = {
            // TODO, give the timeseries the tickername
            id: "timeseries",
            alias: "Timeseries for TICKER_HERE",
            columns: timeseries_columns
        };

        schemaCallback([tableSchema]);
    };

    // Download the data
    myConnector.getData = function (table, doneCallback) {
        apicall = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=5min&apikey=demo"
        $.getJSON(apicall, function (resp) {
            tableData = [];
            // Iterate over the JSON object
            var index = 0;
            for (data_metadata in resp) {
                if (index == 1) {
                    for (timeseries in resp[data_metadata]) {
                        for (data in resp[data_metadata][timeseries]) {
                            tableData.push({
                                "timestamp": timeseries,
                                "open": resp[data_metadata][timeseries]["1. open"],
                                "high": resp[data_metadata][timeseries]["2. high"],
                                "low": resp[data_metadata][timeseries]["3. low"],
                                "close": resp[data_metadata][timeseries]["4. close"],
                                "volume": resp[data_metadata][timeseries]["5. volume"]
                            });
                        }

                        document.getElementById("output").innerHTML = timeseries;
                    }
                }
                index = index + 1;

            }
            table.appendRows(tableData);
            doneCallback();
        });
    };

    tableau.registerConnector(myConnector);
    // Create event listeners for when the user submits the form
    $(document).ready(function () {
        $("#submitButton").click(function () {
            var dateObj = {
                startDate: $('#ticker-list').val().trim(),
            };
            tableau.connectionData = JSON.stringify(dateObj); // Use this variable to pass data to your getSchema and getData functions
            //TODO Ticker name here
            tableau.connectionName = "Ticker feed"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();
