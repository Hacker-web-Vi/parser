$(document).ready(function () {
        $.ajax({
            url: 'metrics.json',
            dataType: 'json',
            cache: false,
            success: function (jsonData) {
                var tableData = [];
                var validatorsToDisplay = jsonData.validators;
                validatorsToDisplay.forEach(function (validator, index) {
                    var totalActiveBlocks = validator.total_signed_blocks + validator.total_missed_blocks
    
                    if (totalActiveBlocks > 0) {
                    validatorUptime = (validator.total_signed_blocks / totalActiveBlocks * 100).toFixed(2);
    
                    } else {
                    var validatorUptime = 0.00;
                    }
                    validatorUptime = parseFloat(validatorUptime);
    
                    var totalJails = validator.slashes ? validator.slashes.length : 0;
    
                    tableData.push([
                        index + 1,
                        validator.valoper,
                        validatorUptime,
                        totalActiveBlocks,
                        validator.total_signed_blocks,
                        validator.total_missed_blocks,
                        validator.total_proposed_blocks,
                        totalJails,
                        validator.delegators_count,
                    ]);
                });
                var table = $('#metrics').DataTable({
                    data: tableData,
                    lengthMenu: [[1000], [1000]],
                    order: [],
                    rowCallback: function (row, data, index) {
                        $('td:eq(0)', row).html(index + 1);
    
                        var jails = data[7];
                        if (jails === 0) {
                            $('td:eq(11)', row).html('<span style="color: green;">' + jails + '</span>');
                        } else {
                            $('td:eq(11)', row).html('<span style="color: red;">' + jails + '</span>');
                        }
                    },
                    columnDefs: [
                        { targets: [16], orderable: false },
                        { targets: [0], orderable: false }
                    ]
                });
                // Add cursor pointer class on hover for 'Voted Proposals #' cell
                var cellIndices = [7];
    
                // Add cursor pointer class on hover for specific cells
                $('#metrics tbody').on('mouseenter mouseleave', '> tr > td', function() {
                    var index = $(this).index() + 1; // Get the 1-based index of the current td
            
                    // Check if the current td index is in the cellIndices array
                    if (cellIndices.includes(index)) {
                        $(this).toggleClass('cursor-pointer');
                    }
                });
                // Initialize variable to store the currently shown row
                var currentShownRow = null;
    
                // Event listener for 'Total Jails' cell
                $('#metrics tbody').on('click', 'td:nth-child(11)', function () {
                    var tr = $(this).closest('tr');
                    var row = table.row(tr);
    
                    // Close current shown row if it's different from the clicked row
                    if (currentShownRow !== null && currentShownRow.index() !== row.index()) {
                        currentShownRow.child.hide();
                        $(currentShownRow.node()).removeClass('shown'); // Convert to jQuery object here
                    }
    
                    if (row.child.isShown()) {
                        // This row is already open - close it
                        row.child.hide();
                        tr.removeClass('shown');
                        currentShownRow = null;
                    } else {
                        // Open this row to show slashing info details
                        var validatorIndex = row.index();
                        var slashingInfo = validatorsToDisplay[validatorIndex].slashing_info;
    
                        // Check if slashingInfo is defined and not null
                        if (slashingInfo && slashingInfo.length > 0) {
                            // Construct HTML for the details
                            var detailsHtml = '<table cellpadding="5" cellspacing="0" border="0" style="padding-left:50px;">';
                            detailsHtml += '<tr><th>Slashing Height</th><th>Time</th></tr>';
    
                            // Iterate over the slashing info
                            slashingInfo.forEach(function (info) {
                                var height = info.height;
                                var time = info.time;
                                detailsHtml += '<tr><td>' + height + '</td><td>' + time + '</td></tr>';
                            });
    
                            detailsHtml += '</table>';
    
                            // Show the details in a child row
                            row.child(detailsHtml).show();
                            tr.addClass('shown');
                            currentShownRow = row; // Store as DataTables row object
                        }
                    }
                });
                // Event listener for 'Jails [1M>]' cell
                $('#metrics tbody').on('click', 'td:nth-child(12)', function () {
                    var tr = $(this).closest('tr');
                    var row = table.row(tr);
    
                    // Close current shown row if it's different from the clicked row
                    if (currentShownRow !== null && currentShownRow.index() !== row.index()) {
                        currentShownRow.child.hide();
                        $(currentShownRow.node()).removeClass('shown'); // Convert to jQuery object here
                    }
    
                    if (row.child.isShown()) {
                        // This row is already open - close it
                        row.child.hide();
                        tr.removeClass('shown');
                        currentShownRow = null;
                    } else {
                        // Open this row to show filtered slashing info details
                        var validatorIndex = row.index();
                        var slashingInfo = validatorsToDisplay[validatorIndex].slashing_info;
    
                        // Check if slashingInfo is defined and not null
                        if (slashingInfo && slashingInfo.length > 0) {
                            // Filter slashing info above height 1000000
                            var filteredSlashings = slashingInfo.filter(function (slashing) {
                                return parseInt(slashing.height) > 1000000;
                            });
    
                            // Construct HTML for the details
                            var detailsHtml = '<table cellpadding="5" cellspacing="0" border="0" style="padding-left:50px;">';
                            detailsHtml += '<tr><th>Slashing Height</th><th>Time</th></tr>';
    
                            // Iterate over the filtered slashing info
                            filteredSlashings.forEach(function (info) {
                                var height = info.height;
                                var time = info.time;
                                detailsHtml += '<tr><td>' + height + '</td><td>' + time + '</td></tr>';
                            });
    
                            detailsHtml += '</table>';
    
                            // Show the details in a child row
                            row.child(detailsHtml).show();
                            tr.addClass('shown');
                            currentShownRow = row; // Store as DataTables row object
                        }
                    }
                });
                // Event listener for 'Val Creation Height' cell
                $('#metrics tbody').on('click', 'td:nth-child(16)', function () {
                    var tr = $(this).closest('tr');
                    var row = table.row(tr);
    
                    // Close current shown row if it's different from the clicked row
                    if (currentShownRow !== null && currentShownRow.index() !== row.index()) {
                        currentShownRow.child.hide();
                        $(currentShownRow.node()).removeClass('shown'); // Convert to jQuery object here
                    }
    
                    if (row.child.isShown()) {
                        // This row is already open - close it
                        row.child.hide();
                        tr.removeClass('shown');
                        currentShownRow = null;
                    } else {
                        // Open this row to show Val Creation Height details
                        var validatorIndex = row.index();
                        var validatorCreationInfo = validatorsToDisplay[validatorIndex].validator_creation_info;
    
                        if (validatorCreationInfo) {
                            var creationHeight = validatorCreationInfo.height;
                            var txHash = validatorCreationInfo.tx_hash;
    
                            // Construct HTML for the details
                            var detailsHtml = '<table cellpadding="5" cellspacing="0" border="0" style="padding-left:50px;">';
                            detailsHtml += '<tr><th>Validator Creation Height</th><th>Transaction Hash</th></tr>';
                            detailsHtml += '<tr><td>' + creationHeight + '</td><td>' + txHash + '</td></tr>';
                            detailsHtml += '</table>';
    
                            // Show the details in a child row
                            row.child(detailsHtml).show();
                            tr.addClass('shown');
                            currentShownRow = row; // Store as DataTables row object
                        }
                    }
                });
                // Event listener for 'Moniker' cell to show details
                $('#metrics tbody').on('click', 'td:nth-child(2)', function () {
                    var tr = $(this).closest('tr');
                    var row = table.row(tr);
    
                    // Close current shown row if it's different from the clicked row
                    if (currentShownRow !== null && currentShownRow.index() !== row.index()) {
                        currentShownRow.child.hide();
                        $(currentShownRow.node()).removeClass('shown');
                    }
    
                    if (row.child.isShown()) {
                        // This row is already open - close it
                        row.child.hide();
                        tr.removeClass('shown');
                        currentShownRow = null;
                    } else {
                        // Open this row to show detailed information
                        var validatorIndex = row.index();
                        var validatorDetails = validatorsToDisplay[validatorIndex];
    
                        // Construct HTML for the details
                        var detailsHtml = '<div class="details-container">';
                        detailsHtml += '<p><strong>Moniker:</strong> ' + validatorDetails.moniker + '</p>';
                        detailsHtml += '<p><strong>Valoper:</strong> ' + validatorDetails.valoper + '</p>';
                        detailsHtml += '<p><strong>Consensus Pub Key:</strong> ' + validatorDetails.consensus_pubkey + '</p>';
                        detailsHtml += '<p><strong>Wallet:</strong> ' + validatorDetails.wallet + '</p>';
                        detailsHtml += '<p><strong>Valcons:</strong> ' + validatorDetails.valcons + '</p>';
                        detailsHtml += '<p><strong>HEX:</strong> ' + validatorDetails.hex + '</p>';
                        detailsHtml += '</div>';
    
                        // Show the details in a child row
                        row.child(detailsHtml).show();
                        tr.addClass('shown');
                        currentShownRow = row; // Store as DataTables row object
                    }
                });
            }
        });
    });