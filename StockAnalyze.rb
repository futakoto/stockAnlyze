#!/usr/bin/ruby
require 'bundler'
Bundler.require

require "rubygems"
require 'pg'
require "csv"
require 'kconv'
require 'open-uri'
require 'logger'
require 'yaml'

DIR_DATA = "public_html/"

def download(address, fileName)

	@path = address

	open(fileName, 'wb') do |output|
	  open(@path) do |data|
	    output.write(data.read)
	  end
	end
end

def update(connection, log, fileName)

 	begin
		index = 1

		query = 'SELECT dstock_ins($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)'

 		basename =  File.basename(fileName)
		date = basename[7,4] + basename[12,2] + basename[15,2]

		CSV.foreach(fileName, encoding: "Shift_JIS:UTF-8") do |row|
  			case index
  			when 1
  			else
          		codes = row[0].split("-")
          		brandcd = codes[0]
          		marketcd = codes[1]

          		begin
            		connection.exec(query,
                    [date,
                    brandcd,
                    row[1],
                    marketcd,
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8]])
           		rescue => ex
                	log.warn(ex)
      	   		end
  			end

  			index += 1
  	end

  	rescue => exc
      log.fatal(exc)
  	end
end

def CreateListingFile(connection, kind)

    query = 'SELECT * FROM get_listing_list($1)' 
    result = connection.exec(query, [kind])

    filename = 'listing' + kind.to_s

	index = 1
	open(DIR_DATA  + filename + '.json', 'w') do |output|
		output.puts('{ data: [ ')

		result.each do |row|
			if index > 1
				output.print(',')
			end
			
			output.puts('	{')
			output.puts('		"brandcd":' + row['brandcd'] + ',')
			output.puts('		"brandname":"' + row['brandname'] + '",')
            output.puts('		"stockdate":' + row['stockdate'])
			output.puts('	}')

			index += 1
		end
		output.puts(']}')
	end
end

def CreateStockDataFile(connection, indexno)
 
    query = 'SELECT * FROM get_stockup_list($1)' 
    result = connection.exec(query, [indexno])
 
    filename =   'stockup'
    if indexno > 0 
    	filename = filename + indexno.to_s
    end 
    filename =  filename + '.json'

	index = 1
	open(DIR_DATA  + filename , 'w') do |output|
		output.puts('{ data: [ ')
		
		result.each do |row|
			if index > 1
				output.print(',')
			end
				
			output.puts('	{')
			output.puts('		"stockdate":' + row['stockdate'] + ',')
			output.puts('		"brandcd":' + row['brandcd'] + ',')
			output.puts('		"brandname":"' + row['brandname'] + '",')
            output.puts('		"fin":' + row['fin'].to_s + ',')
            output.puts('		"turnover":' + row['turnover'].to_s)
			output.puts('	}')

			index += 1
		end
		output.puts(']}')

	end
end

def CreateUpdateFile(connection)

    query = 'SELECT * FROM get_update_stockdate()' 
    result = connection.exec(query)

    stockdate = nil
	result.each do |row|
	    stockdate = row['p_stockdate']
	end
     
    html = '<h4>更新日:' + stockdate + '</h4>'
     
	open(DIR_DATA  +  'update.html', 'w') do |output|
        output.puts(html)
    end
    
end

begin
    log = Logger.new('stockanlyze.log', 7) 
	log.level = Logger::INFO

    log.info('start')

    dbconf = YAML.load_file("./database.yml")["db"]["product"]
    connection = PGconn.open(dbconf)
    connection.internal_encoding = "UTF-8"

    yyyymmdd = Time.now.strftime("%Y-%m-%d")
    path = "http://k-db.com/stocks/" + yyyymmdd + "?download=csv"
    fileName = "stocks_" + yyyymmdd + ".csv"

    download(path, fileName)

    log.info('  update  ')

    update(connection, log, fileName)

    File.delete(fileName)

    log.info('  analyze')
    
    connection.transaction do |connection|
          connection.exec( 'SELECT excute_analyze()')
    end

    CreateListingFile(connection, 1)
    CreateListingFile(connection, 2)
    for index in 0..4
        CreateStockDataFile(connection, index)
    end
    CreateUpdateFile(connection)
 
 log.info('end')    
 
rescue => exc
	log.fatal(exc)
ensure
  if nil != connection
    connection.close
  end
end
