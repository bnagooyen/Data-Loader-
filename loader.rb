# Brian Nguyen 
# Retention Science Code Review
require 'csv'
require 'mysql2'

class DataLoader

  # @param filename [String] client's purchases.csv file
  def initialize(filename, usersFile, ordersFile)
    @filename = filename
    @usersFile = usersFile
    @ordersFile = ordersFile
    @processed = {
      :users  => nil,
      :orders => nil
    }
  end

  def run!
    preprocess_file
    load_files_to_db
  end


  private

  # Takes the input file and produces two TSV files, ready to load into DB
  # This is a good place to use Ruby for:
  # - data cleaning
  # - data transformations
  def preprocess_file
   
    userDataArray = Array.new
    orderDataArray = Array.new
    CSV.foreach(@filename, converters: :numeric, headers:true) do |row|
      
        userHash = {
          :email => row['email'], 
          :full_name => row['full_name'] , 
          :address1 => row['address1'], 
          :city => row['city'], 
          :state => row['state'], 
          :zip => row['zip']
        }

        userDataArray << userHash

    end

     CSV.foreach(@filename, converters: :numeric, headers:true) do |row|
      
        orderHash = {
          :id => row['order_id'],
          :ordered_at => row['order_date'],
          :price => row['price'],
          :user_email => row['email'],
          :item_id => row['item_id']
        }

        orderDataArray << orderHash

    end

    CSV.open(@usersFile, 'w') do |row|

        userDataArray.each do |userRow|
          row << userRow.values
        end
    end

    CSV.open(@ordersFile, 'w') do |row|

        orderDataArray.each do |orderRow|
          row << orderRow.values
        end
    end

      @processed[:users] = @usersFile
      @processed[:orders] = @ordersFile

      
  end

  # Inserts any new records / Updates any existing records
  def load_files_to_db
    con = Mysql2::Client.new(:username=>'root', :host=>'localhost', :password=>'', :database=>'project_db', :local_infile => true)
    
    #loads data from file and loads into tables 
    con.query("LOAD DATA LOCAL INFILE '#{@processed[:users]}' IGNORE INTO TABLE users FIELDS TERMINATED BY ',' (email, full_name, address1, city, state, zip);")
    con.query("DROP TABLE IF EXISTS tmp_import;")
    
    #create a temporary table to be able to insert correct values into orders table 
    con.query("CREATE TABLE `tmp_import` (
                `id` int(11) NOT NULL AUTO_INCREMENT,
                `order_id` int(11) NOT NULL,
                `ordered_at` datetime DEFAULT NULL,
                `email` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                `price` decimal(15,2) DEFAULT NULL,
                `item_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                PRIMARY KEY (`id`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;")
    
    con.query("LOAD DATA LOCAL INFILE '#{@processed[:orders]}'  INTO TABLE tmp_import FIELDS TERMINATED BY ',' (order_id, ordered_at, price, email, item_id);")
    
    #after loading data into temporary table, delete rows where all columns are exactly the same.  
    con.query("DELETE t1.* FROM tmp_import as t1
                    INNER JOIN(
                        SELECT MIN(id), id, order_id, ordered_at, email, price, item_id
                        FROM tmp_import
                        GROUP BY id, order_id, ordered_at, email, price, item_id
                        HAVING COUNT(*) = 1
                                ) AS t2 ON 
                                  t1.order_id = t2.order_id AND 
                                  t1.ordered_at = t2.ordered_at AND
                                  t1.email = t2.email AND
                                  t1.price = t2.price AND
                                  t1.item_id = t2.item_id AND
                                  t1.id < t2.id")
   
   #insert from temporary table into orders table, ignore duplicates so that error does not occur when run for CSV files with new data and some repeats
     con.query("INSERT IGNORE INTO 
              orders(id, user_id, ordered_at, order_amount, number_items) 
              SELECT DISTINCT
                    tmp_import.order_id, 
                    users.id, 
                    ordered_at, 
                    SUM(price), 
                    COUNT(*) 
              FROM tmp_import
              INNER JOIN users ON tmp_import.email = users.email
              GROUP BY tmp_import.order_id, users.id, ordered_at;")

     #drop temp table
     con.query("DROP TABLE tmp_import")

  end

end

 loader = DataLoader.new('purchases.csv', 'users.tsv', 'orders.tsv')
 loader.run!
