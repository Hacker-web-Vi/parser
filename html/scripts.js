
$(document).ready(function () {
    $.ajax({
        url: 'metrics.json',
        dataType: 'json',
        cache: false,
        success: function (jsonData) {
            var tableData = [];
            var validatorsToDisplay = jsonData.validators;

            validatorsToDisplay.forEach(function (validator, index) {
                var totalActiveBlocks = validator.total_signed_blocks + validator.total_missed_blocks;

                // Calculate Uptime
                var validatorUptime;
                if (totalActiveBlocks > 0) {
                    validatorUptime = (validator.total_signed_blocks / totalActiveBlocks * 100).toFixed(2);
                } else {
                    validatorUptime = 0.00;
                }

                validatorUptime = parseFloat(validatorUptime);

                // Total Jails and Slashes Calculation
                var totalJails = validator.slashes ? validator.slashes.length : 0;
                var slashes = validator.slashes || []; // Get slashes if they exist, otherwise an empty array

                tableData.push([
                    index + 1,                                      // Row Number
                    validator.valoper,                              // Valoper
                    validatorUptime,                                // Uptime %
                    totalActiveBlocks,                              // Blocks Active
                    validator.total_signed_blocks,                  // Signed Blocks
                    validator.total_missed_blocks,                  // Missed Blocks
                    validator.total_proposed_blocks,                // Proposed Blocks
                    totalJails,                                     // Total Jails
                    validator.delegators_count,                     // Delegators
                    slashes                                        // Slashes (extra data to use later)
                ]);
            });

            var table = $('#metrics').DataTable({
                data: tableData,
                lengthMenu: [[1000], [1000]],
                order: [], // Disable initial sorting
                rowCallback: function (row, data, index) {
                    $('td:eq(0)', row).html(index + 1);

                    var jails = data[7];
                    if (jails === 0) {
                        $('td:eq(7)', row).html('<span style="color: green;">' + jails + '</span>');
                    } else {
                        $('td:eq(7)', row).html('<span style="color: red;">' + jails + '</span>');
                    }
                },
                columnDefs: [
                    { targets: [1], orderable: false }
                ]
            });

            var cellIndices = [8];
    
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
            $('#metrics tbody').on('click', 'td:nth-child(8)', function () {
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
                    var slashingInfo = validatorsToDisplay[validatorIndex].slashes;

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
        }
    });
});