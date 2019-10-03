(function () {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function (schemaCallback) {
        // Get the list of symbols
        var list_of_schemas = new Array();
        query_data = JSON.parse(tableau.connectionData);

        switch (JSON.parse(tableau.connectionData).type) {
            case "stock-timeseries":
                var symbol_list = query_data.symbol_list.split(",");
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
                        id: symbol_list[symbol],
                        alias: "Timeseries for " + symbol_list[symbol],
                        columns: timeseries_columns,
                    })
                }
                break;
            case "forex":
                var forex_columns = [{
                    id: "currency_pair",
                    alias: "Currency Pair",
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
                }];
                // Have TODO for crypto too - symbol & market
                list_of_schemas.push({
                    id: query_data.from_currency + "_" + query_data.to_currency,
                    alias: "Timeseries for " + query_data.from_currency + "-" + query_data.to_currency,
                    columns: forex_columns,
                })
                break;
            case "technical-indicator":
                var symbol_list = query_data.symbol_list.split(",");
                for (symbol in symbol_list) {
                    var indicator_columns = [{
                        id: "symbol",
                        alias: "symbol",
                        dataType: tableau.dataTypeEnum.string
                    }, {
                        id: "timestamp",
                        dataType: tableau.dataTypeEnum.datetime
                    }, {
                        id: query_data.function_key,
                        dataType: tableau.dataTypeEnum.string
                    }, {
                        id: "arguments",
                        dataType: tableau.dataTypeEnum.string
                    }];
                    list_of_schemas.push({
                        id: symbol_list[symbol] + "_" + query_data.function_key,
                        alias: query_data.function_key + " for " + symbol_list[symbol],
                        columns: indicator_columns,
                    })
                }
                break;

        }
        // Forex just replace symbol with currency pair in format USD-BTZ & removevolume
        // Tech indicator just use symbol, indicator, and datetimestamp
        // sector uhhh idk
        // custom query... might have to do the same shit as the regular query
        schemaCallback(list_of_schemas);
    };

    // Download the data
    myConnector.getData = function (table, doneCallback) {
        var query_data = JSON.parse(tableau.connectionData);
        apicall = create_apicall(table.tableInfo, query_data);
        $.getJSON(apicall, function (resp) {
            var tableData = [];
            var index = 0;
            // Could and should this code be refactored?.... Yes.... Yes it should be 
            var table_data = map_data_to_schema(query_data, resp, table.tableInfo);
            table.appendRows(table_data);
            doneCallback();
        });
    };

    tableau.registerConnector(myConnector);
    // Create event listeners for when the user submits the form
    $(document).ready(function () {
        $("#stock-submit-btn").click(function () {
            if (is_valid_inputs("#stock-submit-btn")) {
                var query_data = {
                    symbol_list: $('#symbol-list').val().trim().replace(/\s/g, ''),
                    api_key: $('#api-key').val().trim().replace(/\s/g, ''),
                    type: "stock-timeseries",
                    function_key: $('#function-key').val().trim(),
                    cadence: $('#intraday-cadence').val().trim(),
                };
                var connection_name = query_data.symbol_list + " Time Series";
                send_tableau(query_data, connection_name);
            }
        });
    });
    $(document).ready(function () {
        $("#forex-submit-btn").click(function () {
            if (is_valid_inputs("#forex-submit-btn")) {
                var query_data = {
                    from_currency: $('#from-currency').val().trim().replace(/\s/g, ''),
                    to_currency: $('#to-currency').val().trim().replace(/\s/g, ''),
                    api_key: $('#api-key').val().trim().replace(/\s/g, ''),
                    type: "forex",
                    function_key: $('#function-key-fx').val().trim(),
                    cadence: $('#intraday-cadence-fx').val().trim(),
                };
                var connection_name = query_data.from_currency + " " + query_data.to_currency;
                send_tableau(query_data, connection_name);
            }
        });
    });
    $(document).ready(function () {
        $("#indicator-submit-btn").click(function () {
            if (is_valid_inputs("#indicator-submit-btn")) {
                var query_data = {
                    function_key: $('#technical-indicator').val().trim().replace(/\s/g, ''),
                    indicator_arguments: $('#indicator-arguments').val().trim().replace(/\s/g, ''),
                    api_key: $('#api-key').val().trim().replace(/\s/g, ''),
                    type: "technical-indicator",
                    symbol_list: $('#symbol-list-indicators').val().trim().replace(/\s/g, ''),
                };
                var connection_name = query_data.function_key;
                send_tableau(query_data, connection_name);
            }
        });
    });
    $(document).ready(function () {
        $("#sector-submit-btn").click(function () {
            if (is_valid_inputs("#sector-submit-btn")) {
                var query_data = {
                    api_key: $('#api-key').val().trim().replace(/\s/g, ''),
                    type: "sector",
                    function_key: "SECTOR",
                };
                var connection_name = "Sector Performance";
                send_tableau(query_data, connection_name);
            }
        });
    });
    $(document).ready(function () {
        $("#query-submit-btn").click(function () {
            if (is_valid_inputs("#query-submit-btn")) {
                var query_data = {
                    custom_query: $('#custom-query').val().trim().replace(/\s/g, ''),
                    api_key: $('#api-key').val().trim().replace(/\s/g, ''),
                    type: "custom-query",
                };
                var connection_name = "Custom Query";
                send_tableau(query_data, connection_name);
            }
        });
    });

    function send_tableau(query_data, connection_name) {
        // can't send the data in list form over Tableau's old browser.... Tableau why
        tableau.connectionData = JSON.stringify(query_data); // Use this variable to pass data to your getSchema and getData functions
        tableau.connectionName = connection_name; // This will be the data source name in Tableau
        tableau.submit(); // This sends the connector object to Tableau
    }
})();

function is_valid_inputs(button_name) {
    switch (button_name) {
        case "#stock-submit-btn":
            if (document.getElementById("api-key").value == "") {
                document.getElementById("stock-output").innerHTML = "Please enter API Key";
                return false;
            }
            if (document.getElementById("symbol-list").value == "" || document.getElementById("api-key").value == "") {
                document.getElementById("stock-output").innerHTML = "Please enter symbols";
                return false;
            }
            break;
        case "#forex-submit-btn":
            if (document.getElementById("api-key").value == "") {
                document.getElementById("forex-output").innerHTML = "Please enter API Key";
                return false;
            }
            if (document.getElementById("from-currency").value == "" || document.getElementById("to-currency").value == "") {
                document.getElementById("forex-output").innerHTML = "Please enter currencies";
                return false;
            }
            break;
        case "#indicator-submit-btn":
            if (document.getElementById("api-key").value == "") {
                document.getElementById("indicator-output").innerHTML = "Please enter API Key";
                return false;
            }
            if (document.getElementById("technical-indicator").value == "" || document.getElementById("indicator-arguments").value == "" || document.getElementById("symbol-list-indicators").value == "") {
                document.getElementById("indicator-output").innerHTML = "Please enter symbol(s), indicator shorthand, and arguments";
                return false;
            }
            break;
        case "#sector-submit-btn":
            if (document.getElementById("api-key").value == "") {
                document.getElementById("sector-output").innerHTML = "Please enter API Key";
                return false;
            }
            break;
        case "#query-submit-btn":
            if (document.getElementById("api-key").value == "") {
                document.getElementById("query-output").innerHTML = "Please enter API Key";
                return false;
            }
            if (document.getElementById("custom-query").value == "") {
                document.getElementById("query-output").innerHTML = "Please enter full query";
                return false;
            }
            break;
        default:
            return true;
    }
    return true;
}

// These can and should go in one function instead of two big ones
// Hashtag DRY 
$(document).ready(function () {
    $("#function-key").change(function () {
        var function_key = $('#function-key').val();
        if (function_key === "TIME_SERIES_INTRADAY") {
            $("#intraday-cadence").show();
            $("#cadence-label").show();
        }
        else if (function_key !== "TIME_SERIES_INTRADAY") {
            $("#intraday-cadence").hide();
            $("#cadence-label").hide();
        }
    });
});

$(document).ready(function () {
    $("#function-key-fx").change(function () {
        var function_key_fx = $('#function-key-fx').val();
        if (function_key_fx === "FX_INTRADAY") {
            $("#intraday-cadence-fx").show();
            $("#cadence-label-fx").show();
            $("#crypto-disclaimer").show();
        }
        else if (function_key_fx !== "TIME_SERIES_INTRADAY") {
            $("#intraday-cadence-fx").hide();
            $("#cadence-label-fx").hide();
            $("#crypto-disclaimer").hide();
        }
    });
});

function map_data_to_schema(query_data, resp, tableinfo) {
    var table_data = [];
    var index = 0;
    // Could and should this code be refactored?.... Yes.... Yes it should be 
    switch (query_data.type) {
        case "stock-timeseries":
            for (data_metadata in resp) {
                if (index == 1) {
                    for (timeseries in resp[data_metadata]) {
                        table_data.push({
                            "symbol": tableinfo.id,
                            "timestamp": timeseries,
                            "open": resp[data_metadata][timeseries]["1. open"],
                            "high": resp[data_metadata][timeseries]["2. high"],
                            "low": resp[data_metadata][timeseries]["3. low"],
                            "close": resp[data_metadata][timeseries]["4. close"],
                            "volume": resp[data_metadata][timeseries]["5. volume"]
                        });
                    }
                }
                index = index + 1;
            }
            return table_data;
            break;
        case "forex":
            for (data_metadata in resp) {
                if (index == 1) {
                    for (timeseries in resp[data_metadata]) {
                        table_data.push({
                            "currency_pair": query_data.from_currency + "-" + query_data.to_currency,
                            "timestamp": timeseries,
                            "open": resp[data_metadata][timeseries]["1. open"],
                            "high": resp[data_metadata][timeseries]["2. high"],
                            "low": resp[data_metadata][timeseries]["3. low"],
                            "close": resp[data_metadata][timeseries]["4. close"],
                        });
                    }
                }
                index = index + 1;
            }
            return table_data;
            break;
        case "technical-indicator":
            for (data_metadata in resp) {
                if (index == 1) {
                    for (timeseries in resp[data_metadata]) {
                        table_data.push({
                            "symbol": tableinfo.id.split("_")[0],
                            "timestamp": timeseries,
                            [query_data.function_key]: resp[data_metadata][timeseries][query_data.function_key],
                            "arguments": query_data.indicator_arguments,
                        });
                    }
                }
                index = index + 1;
            }
            return table_data;
        default:

    }
}

function create_apicall(tableInfo, query_data) {
    var baseurl = "https://www.alphavantage.co/query?function=";
    var cadence = "";
    if (query_data.function_key.indexOf("INTRADAY") !== -1) {
        cadence = "&interval=" + query_data.cadence;
    }
    switch (query_data.type) {
        case "stock-timeseries":
            return baseurl + query_data.function_key + "&symbol=" + tableInfo.id + cadence + "&apikey=" + query_data.api_key;
            break;
        case "forex":
            return baseurl + query_data.function_key + "&from_symbol=" + query_data.from_currency + "&to_symbol=" + query_data.to_currency + cadence + "&apikey=" + query_data.api_key;
            break;
        case "technical-indicator":
            return baseurl + query_data.function_key + "&symbol=" + tableInfo.id.split("_")[0] + query_data.indicator_arguments + "&apikey=" + query_data.api_key;
            break;
        default:
            break;
    }
}