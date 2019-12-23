% Author: Mohammad Dehghani Ashkezari <mdehghan@uw.edu>
% 
% Date: 2019-12-17
% 
% Function: Constructs the fundamentals of CMAP API request submissions.
%
% This package is adopted from 'pycmap' which is the python client of Simons 
% CMAP ecosystem (https://github.com/simonscmap/pycmap).   




classdef CMAP
    % Handles RESTful requests to the Simons CMAP API.    
    
    properties
        apiKey 
    end

    
    methods
        function obj = CMAP(token)
            % CMAP constructor method.
            % :param str token: access token to make client requests.
            if nargin < 1
                token = CMAP.get_api_key();                
            end    
            obj.apiKey = token;
        end
        
        function obj = set.apiKey(obj, value)
            obj.apiKey = value;
            CMAP.set_api_key(value); 
        end      
        
    end
    
    
    methods (Static)       
        function apiKey = get_api_key()
            % Returns CMAP API Key previously stored in a system variable (see set_api_key(api_key) function).
            apiKey = getenv('CMAP_API_KEY');
            if isempty(apiKey)
                error('\n\n%s \n%s \n%s \n%s\n',... 
                      'CMAP API Key not found.',... 
                      'You may obtain an API Key from https://simonscmap.com.',... 
                      'Record your API key on your machine permanently using the following command:',...
                      'CMAP.set_api_key(''<Your API Key>'');'...
                      )
            end   
        end

        function set_api_key(api_key)
            % Stores the API Key in a system variable.
            CMAP.apiKey = api_key;
            setenv('CMAP_API_KEY', api_key);
        end    
        

        function queryString = encode_payload(payload)
            % Constructs the encoded query string to be added to the base API URL
            % (domain+route). payload holds the query parameters and their values.
            fn = fieldnames(payload);
            queryString = '';
            for k=1:numel(fn)
                queryString = strcat(queryString, fn{k}, '=', payload.(fn{k}));
                if k < numel(fn)
                    queryString = strcat(queryString, '&');
                end    
            end
            queryString = strrep(queryString, ' ', '%20');
        end

        function tbl = atomic_request(route, payload)
            % Submits a single GET request. 
            % Returns the body in form of a MATALAB table if 200 status.
            import matlab.net.*
            import matlab.net.http.*

            baseURL = 'https://simonscmap.com';
            queryString = CMAP.encode_payload(payload);
            uri = strcat(baseURL, route, queryString);          
            r = RequestMessage('GET');
            prefixeKey = char(strcat('Api-Key', {' '}, CMAP.get_api_key()));
            field = matlab.net.http.field.GenericField('Authorization', prefixeKey);
            r = addFields(r, field);            
            options = matlab.net.http.HTTPOptions('ConnectTimeout', 2000);            
            [resp, ~, hist] = send(r, uri, options);
            status = getReasonPhrase(resp.StatusCode);
            tbl = CMAP.resp_to_table(resp.Body.Data);    
             if ~strcmp(char(status), 'OK')
                 disp(strcat('Status:', {' '}, status))
                 disp(strcat('Status Code:', {' '}, num2str(resp.StatusCode)))
                 disp(strcat('Message:', {' '}, char(resp.Body.Data)'))
             end    
        end

        
        function tbl = resp_to_table(respData)
            % save the response data in a csv file. 
            % the csv file is then deleted after is read into a table variable.
            % TODO: see if it's possible to directly convert the response to a table?
            % TODO: resp.Body.Data >> table variable
            fname = 'resp.csv';
            fid = fopen(fname, 'wt');
            fwrite(fid, respData);
            fclose(fid);
            tbl = readtable(fname);
            delete(fname);
        end
        
        
        
        function tbl = query(queryString)
            % Takes a custom query and returns the results in form of a table.
            payload = struct('query', queryString);
            tbl = CMAP.atomic_request('/api/data/query?', payload);
        end
        
        
        function tbl = stored_proc(args)
            % Executes a strored-procedure and returns the results in form of a table.
            payload = struct('tableName', args(1), 'fields', args(2), 'dt1', args(3), 'dt2', args(4), 'lat1', args(5), 'lat2', args(6), 'lon1', args(7), 'lon2', args(8), 'depth1', args(9), 'depth2', args(10), 'spName', args(11));
            tbl = CMAP.atomic_request('/api/data/sp?', payload);
        end
        
        
        function tbl = get_catalog()
            % Returns a table containing full Simons CMAP catalog of variables.
            tbl = CMAP.query('EXEC uspCatalog');
        end
        

        function tbl = search_catalog(keywords)
            % Returns a dataframe containing a subset of Simons CMAP catalog of variables. 
            % All variables at Simons CMAP catalog are annotated with a collection of semantically related keywords. 
            % This method takes the passed keywords and returns all of the variables annotated with similar keywords.
            % The passed keywords should be separated by blank space. The search result is not sensitive to the order of keywords and is not case sensitive.
            % The passed keywords can provide any 'hint' associated with the target variables. Below are a few examples: 
            %
            % * the exact variable name (e.g. NO3), or its linguistic term (Nitrate)
            %    
            % * methodology (model, satellite ...), instrument (CTD, seaflow), or disciplines (physics, biology ...) 
            %    
            % * the cruise official name (e.g. KOK1606), or unofficial cruise name (Falkor)
            %
            % * the name of data producer (e.g Penny Chisholm) or institution name (MIT)
            %
            % If you searched for a variable with semantically-related-keywords and did not get the correct results, please let us know. 
            % We can update the keywords at any point.
            tbl = CMAP.query(sprintf('EXEC uspSearchCatalog ''%s''', keywords));
        end
        
        
        function tbl = datasets()
            % Returns a table containing the list of data sets hosted by Simons CMAP database.
            tbl = CMAP.query('EXEC uspDatasets');
        end
        
        
        function tbl = head(tableName, rows)
            % Returns top records of a data set.
            if nargin < 2
                rows = 5;
            end    
            tbl = CMAP.query(sprintf('EXEC uspHead ''%s'', ''%d''', tableName, rows));
        end

        
        function tbl = columns(tableName)
            % Returns the list of data set columns.
            tbl = CMAP.query(sprintf('EXEC uspColumns ''%s''', tableName));
        end
        
        
        function tbl = get_dataset(tableName)
            % Returns the entire dataset.
            % It is not recommended to retrieve datasets with more than 100k rows using this method.
            % For large datasets, please use the 'space_time' method and retrieve the data in smaller chunks.
            % Note that this method does not return the dataset metadata. 
            % Use the 'get_dataset_metadata' method to get the dataset metadata.
            
            maxRow = 2000000;
            df = CMAP.query(sprintf('SELECT JSON_stats FROM tblDataset_Stats WHERE Dataset_Name=''%s'' ', tableName));
            js = jsondecode(char(df.JSON_stats(1)));
            rows = js.lat.count;
            if isempty(rows)
                error('No size estimates found for the %s table.', tableName)
            end
            if rows > maxRow
                msg = sprintf('The requested dataset has %d records.', rows);
                msg = strcat(msg, sprintf('\nIt is not recommended to retrieve datasets with more than %d rows using this method.\n', maxRow));
                msg = strcat(msg, sprintf('\nFor large datasets, please use the ''space_time'' method and retrieve the data in smaller chunks.'));
                error(msg)
            end    
            tbl = CMAP.query(sprintf("SELECT * FROM %s", tableName));
        end
        
        
        function tbl = get_dataset_metadata(tableName)
            % Returns a table containing the data set metadata.
            tbl = CMAP.query(sprintf('EXEC uspDatasetMetadata ''%s''', tableName));
        end
        
        
        function tbl = get_var_catalog(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing all of the variable's info at catalog.
            query = sprintf('SELECT * FROM [dbo].udfCatalog() WHERE Table_Name=''%s'' AND Variable=''%s''', tableName, varName);
            tbl = CMAP.query(query);
        end
        

        function tbl = get_var_long_name(tableName, varName)
            % Returns the long name of a given variable.
            %tbl = char(CMAP.query(sprintf('EXEC uspVariableLongName ''%s'', ''%s''', tableName, varName)).Long_Name);
            tbl = char(CMAP.query(sprintf('SELECT Long_Name, Short_Name FROM tblVariables WHERE Table_Name=''%s'' AND  Short_Name=''%s''', tableName, varName)).Long_Name); ;
        end
        

        function tbl = get_unit(tableName, varName)
            % Returns the unit for a given variable.
            tbl = char(CMAP.query(sprintf('EXEC uspVariableUnit ''%s'', ''%s''', tableName, varName)).Unit);
        end
        

        function tbl = get_var_resolution(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing the variable's spatial and temporal resolutions.
            tbl = CMAP.query(sprintf('EXEC uspVariableResolution ''%s'', ''%s''', tableName, varName));
        end
        

        function tbl = get_var_coverage(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing the variable's spatial and temporal coverage.
            tbl = CMAP.query(sprintf('EXEC uspVariableCoverage ''%s'', ''%s''', tableName, varName));
        end
        

        function tbl = get_var_stat(tableName, varName)
            % Returns a single-row table from catalog (udfCatalog) containing the variable's summary statistics.
            tbl = CMAP.query(sprintf('EXEC uspVariableStat ''%s'', ''%s''', tableName, varName));
        end


        
        function hasField = has_field(tableName, varName)
            % Returns a boolean confirming whether a field (varName) exists in a table (data set).
            query = sprintf('SELECT COL_LENGTH(''%s'', ''%s'') AS RESULT ', tableName, varName);
            df = CMAP.query(query).RESULT;
            hasField = false;
            if ~isempty(df) 
                hasField = true;
            end                
        end
        
        
        function grid = is_grid(tableName, varName)
            % Returns a boolean indicating whether the variable is a gridded product or has irregular spatial resolution.
            grid = true;
            query = sprintf('SELECT Spatial_Res_ID, RTRIM(LTRIM(Spatial_Resolution)) AS Spatial_Resolution FROM tblVariables JOIN tblSpatial_Resolutions ON [tblVariables].Spatial_Res_ID=[tblSpatial_Resolutions].ID WHERE Table_Name=''%s'' AND Short_Name=''%s'' ', tableName, varName);
            df = CMAP.query(query);
            if isempty(df) 
                grid = NaN;
            elseif contains(lower(char(df.Spatial_Resolution)), 'irregular')    
                grid = false;
            end            
            
        end

                
        function clim = is_climatology(tableName)
            % Returns True if the table represents a climatological data set.    
            % Currently, the logic is based on the table name.
            % Ultimately, it should query the DB to determine if it's a climatological data set.
            clim = contains(tableName, '_Climatology');
        end
                
       
        function tbl = get_references(datasetID)
            % Returns a table containing refrences associated with a data set.
            tbl = CMAP.query(sprintf('SELECT Reference FROM dbo.udfDatasetReferences(%d)', datasetID));
        end
        
        
        function tbl = get_metadata(table, variable)
            % Returns a table containing the associated metadata.
            tbl = CMAP.query(sprintf('EXEC uspVariableMetaData ''%s'', ''%s''', table, variable));
        end
        
        
        function tbl = cruises()
            % Returns a table containing a list of the hosted cruise names.
            tbl = CMAP.query('EXEC uspCruises');
        end    
        

        function tbl = cruise_by_name(cruiseName)
            % Returns a table containing cruise info using cruise name.
            tbl = CMAP.query(sprintf('EXEC uspCruiseByName ''%s''', cruiseName));
            [rows ~] = size(tbl);
            if isempty(tbl)
                error('Invalid cruise name: %s', cruiseName);
            end
            if rows > 1
                disp(tbl)
                error('More than one cruise found. Please provide a more specific cruise name. ')
            end
        end    
        

        function tbl = cruise_bounds(cruiseName)
            % Returns a table containing cruise boundaries in space and time.
            df = CMAP.cruise_by_name(cruiseName);
            tbl = CMAP.query(sprintf('EXEC uspCruiseBounds %d', df.ID));
        end    
        
        
        function tbl = cruise_trajectory(cruiseName)
            % Returns a table containing the cruise trajectory.
            df = CMAP.cruise_by_name(cruiseName);
            tbl = CMAP.query(sprintf('EXEC uspCruiseTrajectory %d', df.ID));
        end    
        
        
        function tbl = cruise_variables(cruiseName)
            % Returns a table containing all registered variables (at Simons CMAP) during a cruise.
            df = CMAP.cruise_by_name(cruiseName);
            tbl = CMAP.query(sprintf('SELECT * FROM dbo.udfCruiseVariables(%d)', df.ID));
        end    

        
        
        
        
        
        
        
        function tbl = subset(spName, table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            args = {table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2, spName};
            tbl = CMAP.stored_proc(args);
        end

        
        function tbl = space_time(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % The results are ordered by time, lat, lon, and depth (if exists).
            tbl = CMAP.subset('uspSpaceTime', table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end


        function usp = interval_to_uspName(interval)
            if strcmp(interval, '')
                usp = 'uspTimeSeries';
            elseif any(strcmp(interval, {'w', 'week', 'weekly'}))    
                usp = 'uspWeekly';
            elseif any(strcmp(interval, {'m', 'month', 'monthly'}))    
                usp = 'uspMonthly';
            elseif any(strcmp(interval, {'q', 's', 'season', 'seasonal', 'seasonality', 'quarterly'}))    
                usp = 'uspQuarterly';
            elseif any(strcmp(interval, {'y', 'a', 'year', 'yearly', 'annual'}))    
                usp = 'uspAnnual';
            end                        
        end
        
        
        function tbl = time_series(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2, interval)
            % Returns a subset of data according to space-time constraints.
            % The results are aggregated by time and ordered by time, lat, lon, and depth (if exists).
            % The timeseries data can be binned weekyly, monthly, qurterly, or annualy, if interval variable is set (this feature is not applicable to climatological data sets). 
            
            if nargin < 11
                interval = '';
            end               
            uspName = CMAP.interval_to_uspName(interval);            
            if ~strcmp(uspName, 'uspTimeSeries') && CMAP.is_climatology(table)
                error('\nTable %s represents a climatological data set. \n%s', table, 'Custom binning (monthly, weekly, ...) is not suppoerted for climatological data sets. ')
            end    
            tbl = CMAP.subset(uspName, table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end
        
        
        function tbl = depth_profile(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % The results are aggregated by depth and ordered by depth.
            tbl = CMAP.subset('uspDepthProfile', table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end

        
        function tbl = section(table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2)
            % Returns a subset of data according to space-time constraints.
            % The results are ordered by time, lat, lon, and depth.
            tbl = CMAP.subset('uspSectionMap', table, variable, dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2);           
        end
        
        

        function tbl = match(sourceTable, sourceVar, targetTables, targetVars,... 
             dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2,... 
             temporalTolerance, latTolerance, lonTolerance, depthTolerance)        
        % Colocalizes the source variable (from source table) with the target variable (from target table).
        % The tolerance parameters set the matching boundaries between the source and target data sets. 
        % Returns a table containing the source variable joined with the target variable.
        tbl = Match('uspMatch', sourceTable, sourceVar, targetTables, targetVars,...
                     dt1, dt2, lat1, lat2, lon1, lon2, depth1, depth2,...
                     temporalTolerance, latTolerance, lonTolerance, depthTolerance).compile();
        end

        
        

        function tbl = along_track(cruise, targetTables, targetVars, depth1, depth2, temporalTolerance, latTolerance, lonTolerance, depthTolerance)     
            % Takes a cruise name and colocalizes the cruise track with the specified variable(s).

            df = CMAP.cruise_bounds(cruise);
            tbl = CMAP.match(...
                             'tblCruise_Trajectory',...       % sourceTable
                             string(df.ID(1)),...             % sourceVar
                             targetTables,...                 % targetTables
                             targetVars,...                   % targetVars
                             df.dt1(1),...                    % dt1
                             df.dt2(1),...                    % dt2
                             df.lat1(1),...                   % lat1
                             df.lat2(1),...                   % lat2
                             df.lon1(1),...                   % lon1
                             df.lon2(1),...                   % lon2
                             depth1,...                       % depth1
                             depth2,...                       % depth2
                             temporalTolerance,...            % temporalTolerance
                             latTolerance,...                 % latTolerance
                             lonTolerance,...                 % lonTolerance
                             depthTolerance...                % depthTolerance
                             );
        end
        
    end

    
end

