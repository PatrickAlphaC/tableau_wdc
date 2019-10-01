(function () {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function (schemaCallback) {
        // Get the list of symbols
        var symbol_list = JSON.parse(tableau.connectionData).symbol_list.split(",");
        var list_of_schemas = new Array();
        for (symbol in symbol_list) {
            var timeseries_columns = [{
                id: "symbol",
                alias: "symbol",
                dataType: tableau.dataTypeEnum.string
            }, {
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
            list_of_schemas.push({
                // TODO, give the timeseries the symbolname
                id: symbol_list[symbol],
                alias: "Timeseries for " + symbol_list[symbol],
                columns: timeseries_columns,
                // symbol: symbol_list[symbol]
            })
        }
        schemaCallback(list_of_schemas);
    };

    // Download the data
    myConnector.getData = function (table, doneCallback) {
        var baseurl = "https://www.alphavantage.co/query?function=";
        var function_key = "TIME_SERIES_INTRADAY";
        var query_data = JSON.parse(tableau.connectionData);
        var symbol = table.tableInfo.id;
        var apicall = baseurl + function_key + "&symbol=" + symbol + "&interval=1min&apikey=" + query_data.api_key;
        //var apicall = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=1min&apikey=NL2O0F4U4GBPOQMN";
        $.getJSON(apicall, function (resp) {
            // var tableData = [];
            // for (data_metadata in resp) {
            //     // "Time Series (5min)"
            //     for (timeseries in resp[data_metadata]) {
            //         tableData.push({
            //             "symbol": symbol,
            //             "timestamp": timeseries,
            //             "open": resp[data_metadata][timeseries]["1. open"],
            //             "high": resp[data_metadata][timeseries]["2. high"],
            //             "low": resp[data_metadata][timeseries]["3. low"],
            //             "close": resp[data_metadata][timeseries]["4. close"],
            //             "volume": resp[data_metadata][timeseries]["5. volume"]
            //         });
            //     }
            // }

            var tableData = [];
            var index = 0;
            for (data_metadata in resp) {
                if (index == 1) {
                    for (timeseries in resp[data_metadata]) {
                        tableData.push({
                            "symbol": symbol,
                            "timestamp": timeseries,
                            "open": resp[data_metadata][timeseries]["1. open"],
                            "high": resp[data_metadata][timeseries]["2. high"],
                            "low": resp[data_metadata][timeseries]["3. low"],
                            "close": resp[data_metadata][timeseries]["4. close"],
                            "volume": resp[data_metadata][timeseries]["5. volume"]
                        });
                    }
                    table.appendRows(tableData);
                }
                index = index + 1;
            }
            doneCallback();
        });
    };

    tableau.registerConnector(myConnector);
    // Create event listeners for when the user submits the form
    $(document).ready(function () {
        $("#submitButton").click(function () {
            if (document.getElementById("symbol-list").value == "" || document.getElementById("api-key").value == "") {
                document.getElementById("output").innerHTML = "Please enter symbols and API Key";
            } else {
                var query_data = {
                    symbol_list: $('#symbol-list').val().trim().replace(/\s/g, ''),
                    api_key: $('#api-key').val().trim().replace(/\s/g, ''),
                };
                // can't send it in list form wtf
                tableau.connectionData = JSON.stringify(query_data); // Use this variable to pass data to your getSchema and getData functions
                // TODO symbol name here
                tableau.connectionName = query_data.symbol_list + " Time Series"; // This will be the data source name in Tableau
                tableau.submit(); // This sends the connector object to Tableau
            }
        });
    });
})();
