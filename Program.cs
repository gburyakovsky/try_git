using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.Sql;
using System.Diagnostics.Eventing.Reader;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Remoting;
using System.Globalization;
using System.Web;
using System.Web.UI.HtmlControls;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace BlueDolphin.Renewal
{
    internal class BlueDolphinRenewal
    {
        //debug variable
        public static bool Debug = true;
        // define the database table names used in the project
        public static string TABLE_ADDRESS_BOOK = "address_book";
        public static string TABLE_ADDRESS_FORMAT = "address_format";
        public static string TABLE_BANNERS = "banners";
        public static string TABLE_BANNERS_HISTORY = "banners_history";
        public static string TABLE_CATEGORIES = "categories";
        public static string TABLE_CATEGORIES_DESCRIPTION = "categories_description";
        public static string TABLE_CONFIGURATION = "configuration";
        public static string TABLE_CONFIGURATION_GROUP = "configuration_group";
        public static string TABLE_COUNTER = "counter";
        public static string TABLE_COUNTER_HISTORY = "counter_history";
        public static string TABLE_COUNTRIES = "countries";
        public static string TABLE_CURRENCIES = "currencies";
        public static string TABLE_CUSTOMERS = "customers";
        public static string TABLE_CUSTOMERS_BASKET = "customers_basket";
        public static string TABLE_CUSTOMERS_BASKET_ATTRIBUTES = "customers_basket_attributes";
        public static string TABLE_CUSTOMERS_INFO = "customers_info";
        public static string TABLE_LANGUAGES = "languages";
        public static string TABLE_MANUFACTURERS = "manufacturers";
        public static string TABLE_MANUFACTURERS_INFO = "manufacturers_info";
        public static string TABLE_ORDERS = "orders";
        public static string TABLE_ORDERS_PRODUCTS = "orders_products";
        public static string TABLE_ORDERS_PRODUCTS_ATTRIBUTES = "orders_products_attributes";
        public static string TABLE_ORDERS_PRODUCTS_DOWNLOAD = "orders_products_download";
        public static string TABLE_ORDERS_STATUS = "orders_status";
        public static string TABLE_ORDERS_STATUS_HISTORY = "orders_status_history";
        public static string TABLE_ORDERS_TOTAL = "orders_total";
        public static string TABLE_PRODUCTS = "products";
        public static string TABLE_PRODUCTS_ATTRIBUTES = "products_attributes";
        public static string TABLE_PRODUCTS_ATTRIBUTES_DOWNLOAD = "products_attributes_download";
        public static string TABLE_PRODUCTS_DESCRIPTION = "products_description";
        public static string TABLE_PRODUCTS_GROUPS = "products_groups"; //Separate Pricing per Customer Mod
        public static string TABLE_PRODUCTS_NOTIFICATIONS = "products_notifications";
        public static string TABLE_PRODUCTS_OPTIONS = "products_options";
        public static string TABLE_PRODUCTS_OPTIONS_VALUES = "products_options_values";
        public static string TABLE_PRODUCTS_OPTIONS_VALUES_TO_PRODUCTS_OPTIONS =
            "products_options_values_to_products_options";
        public static string TABLE_PRODUCTS_TO_CATEGORIES = "products_to_categories";
        public static string TABLE_REVIEWS = "reviews";
        public static string TABLE_REVIEWS_DESCRIPTION = "reviews_description";
        public static string TABLE_SKINSITES = "skinsites";
        public static string TABLE_SESSIONS = "sessions";
        public static string TABLE_SPECIALS = "specials";
        public static string TABLE_TAX_CLASS = "tax_class";
        public static string TABLE_TAX_RATES = "tax_rates";
        public static string TABLE_GEO_ZONES = "geo_zones";
        public static string TABLE_ZONES_TO_GEO_ZONES = "zones_to_geo_zones";
        public static string TABLE_WHOS_ONLINE = "whos_online";
        public static string TABLE_ZONES = "zones";
        public static string TABLE_PAYPALIPN_TXN = "paypalipn_txn"; // PAYPALIPN
        // Added for Xsell Products Mod
        public static string TABLE_PRODUCTS_XSELL = "products_xsell";
        public static string TABLE_TOPICS = "topics";
        public static string TABLE_CUSTOMERS_TOPICS = "customers_topics";
        public static string TABLE_CUSTOMERS_TOPICS_HISTORY = "customers_topics_history";
        public static string TABLE_SKUS = "skus";
        public static string TABLE_PAYMENT_CARDS = "payment_cards";
        public static string TABLE_PREMIUMS = "premiums";
        public static string TABLE_PREMIUMS_DESCRIPTION = "premiums_description";
        public static string TABLE_CC_TRANSACTIONS = "cc_transactions";
        public static string TABLE_PRIZE_DRAWING_ENTRIES = "prize_drawing_entries";
        public static string TABLE_ADMIN = "admin";
        public static string TABLE_CUSTOMERS_MEMBER_STATUS_HISTORY = "customers_member_status_history";
        public static string TABLE_FULFILLMENT_BATCH_WEEK = "fulfillment_batch_week";
        public static string TABLE_FULFILLMENT_BATCH = "fulfillment_batch";
        public static string TABLE_FULFILLMENT_BATCH_ITEMS = "fulfillment_batch_items";
        public static string TABLE_FULFILLMENT_STATUS = "fulfillment_status";
        public static string TABLE_PUBLICATION_FREQUENCY = "publication_frequency";
        public static string TABLE_CC_TRANSACTIONS_ORDERS = "cc_transactions_orders";
        public static string TABLE_ORDERS_FULFILLMENT_BATCH_HISTORY = "orders_fulfillment_batch_history";
        public static bool USE_PCONNECT = false; // use persistent connections?
        public static string DEFAULT_PENDING_COMMENT = "Renewal Order has been created.";
        public static string CHARSET = "iso-8859-1";
        public static string FULFILLMENT_FULFILL_ID = "1";
        public static string FULFILLMENT_CHANGE_ADDRESS_ID = "2";
        public static string FULFILLMENT_CANCEL_ID = "3";
        public static string FULFILLMENT_IGNORE_FULFILLMENT_ID = "4";
        public static string PAPER_INVOICE_DATE_FORMAT = "Ymd_His";
        public static string DEFAULT_COUNTRY_ID = "223";
        public static string MODULE_PAYMENT_PAYFLOWPRO_TEXT_ERROR = "Credit Card Error!";
        public static string DATE_FORMAT_DB = "%Y-%m-%d %H:%M:%S";
        public static string FILENAME_PRODUCT_INFO = "product_info.php";
        public static string DEFAULT_ORDERS_STATUS_ID = "null";
        public static string RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS = "0";
        public static string DEFAULT_RENEWAL_CHARGE_DAYS = "0";
        public static string DEFAULT_RENEWAL_LEADTIME;
        public static string DEFAULT_CURRENCY;
        public static string LANGUAGE_CURRENCY;
        public static string USE_DEFAULT_LANGUAGE_CURRENCY;
        public static string MODULE_PAYMENT_PAYFLOWPRO_USER = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_VENDOR = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_PARTNER = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_TRXTYPE = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_TENDER = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_PWD = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_PFPRO_CERT_PATH_ENV = string.Empty;
        public static string MODULE_PAYMENT_PAYFLOWPRO_HOSTADDRESS = string.Empty;
        public static int MAX_RENEWAL_CREDIT_CARD_CHARGE_ATTEMPTS;
        public static int MAX_RENEWAL_CREDIT_CARD_CHARGE_EXPIRATION_FAILURES;
        public static int MODULE_PAYMENT_PAYFLOWPRO_PFPRO_ORDER_STATUS_ID;
        public static int DEFAULT_RETRY_RENEWAL_CHARGE_DAYS;
        public static string SEND_EMAILS;
        public static string customer_notification;
        private static string pfpro_defaultport = string.Empty;
        private static string pfpro_defaulttimeout = string.Empty;
        private static string pfpro_proxyaddress = string.Empty;
        private static string pfpro_proxyport = string.Empty;
        private static string pfpro_proxylogin = string.Empty;
        private static string pfpro_proxypassword = string.Empty;
        private static string pfpro_defaulthost = string.Empty;
        private static string PFPRO_EXE_PATH = string.Empty;
        private static string LD_LIBRARY_PATH_ENV = string.Empty;
        private static string PFPRO_CERT_PATH_ENV = string.Empty;
        //the following are defined in renewal_track_emails table.
        public static string TRACK1 = "1014";
        public static string TRACK2_BAD_CC = "1015";
        public static string TRACK2_CHECK = "1016";
        public static string TRACK2_MC = "1,.017";
        public static string TRACK2_PC = "1018";
        private static string comments;
        public static int number_of_renewal_orders_created;
        public static int number_of_renewal_orders_charged;
        public static int number_of_renewal_invoices_created;
        public static int number_of_additional_renewal_invoices_created;
        public static int number_of_renewal_email_invoices_sent;
        public static int number_of_renewal_paper_invoices_file_records;
        public static int number_of_invoices_cleaned_up;
        public static int number_of_renewal_orders_mass_cancelled;
        private static int order_fulfillment_batch_id;
        private static int is_renewal_order;
        private static int orders_status;
        private static string skus_type;
        private static int skus_type_order_period;
        private static int fulfillment_batch_id;
        /// <summary>
        /// Variables
        /// </summary>
        private static string renewal_skus_type_order_period = string.Empty;
        private static bool update_orders_fulfillment_status_id;
        private static int skus_status;
        private static int skus_type_order;
        private static int products_id;
        private static int continuous_service;
        private static int renewal_order_auto_renew;
        private static int renewal_order_status;
        private static int customers_id;
        private static int orders_id;
        private static int prior_orders_id;
        private static int prior_order_status;
        private static int prior_order_auto_renew;
        private static int auto_renew;
        private static int renewal_orders_id;
        private static int renewals_invoices_id;
        private static int renewals_billing_series_id;
        private static int renewals_billing_series_effort_number;
        private static int number_of_renew_invoices_prepared = 0;
        private static int original_order_products_id = 0;
        private static int original_order_skus_id;
        private static string original_order_skus_type;
        private static int original_order_skus_type_order = 0;
        private static int original_order_skus_type_order_period;
        private static int original_order_id;
        private static int original_products_status;
        // Is it a postcard confirmation?
        private static int original_is_postcard_confirmation;
        private static int renewal_invoices_created;
        private static int renewal_invoices_sent;
        private static double final_price;
        private static int renewal_orders_customers_id;
        private static int products_status;
        private static int skus_id;
        private static int skinsites_id;
        private static int skus_term;
        private static int effort_number;
        private static DateTime date_purchased;
        private static double amount_owed;
        private static double amount_paid;
        private static double price;
        private static int is_gift;
        private static int is_postcard_confirmation;
        private static int renewals_credit_card_charge_attempts;
        private static int renewals_expiration_date_failures;
        private static int payment_cards_id;
        private static int first_issue_delay_days;
        private static int days_spanned;
        private static int fulfillment_batch_items_id;
        private static string billing_first_name;
        private static string billing_last_name;
        private static string billing_address_line_1;
        private static string billing_city;
        private static string billing_state;
        private static string billing_postal_code;
        private static string delivery_first_name;
        private static string delivery_last_name;
        private static string delivery_address_line_1;
        private static string delivery_city;
        private static string delivery_state;
        private static string delivery_postal_code;
        private static string cc_number;
        private static string cc_number_display;
        private static string cc_expires;
        private static string renewals_billing_series_code;
        private static bool accepted_for_delivery;
        private static string renewals_invoices_email_name;
        private static string products_name;
        private static string email_address;
        private static string billing_address;
        private static string billing_postcode;
        private static string template_directory;
        private static string billing_country;
        private static DateTime date_sent;
        private static string override_renewal_billing_descriptor;
        private static string products_billing_descriptor;
        private static string cc_expires_year;
        private static string cc_expires_month;
        private static string query = string.Empty;
        private static bool is_perfect_renewal;
        private static string compare_date;
        private static string check_renewal_order_result;
        private static bool is_gc_order;
        private static string Key = "W1j Witt3 Wy4en W1l13n W3l Warm3 Woll$n WiNter W4nt3n Wa553n";
        private static string suffix;
        private static string authCode;
        private static string the_code;
        private static string renewal_fulfillment_delay;
        private static string fulfillment_delay_batch_week;
        private static string update_fulfill_fields;
        private static DateTime fulfillment_delay_batch_date;
        private static string fulfillment_current_batch_week;
        private static DateTime fulfillment_current_batch_date;
        private static string fulfillment_batch_week;
        private static DateTime fulfillment_batch_date;
        private static string order_fulfillment_batch_week;
        private static int order_fulfillment_status_id;
        private static string currency;
        private static List<string> all_countries_array = new List<string>();
        private static Dictionary<string, object> orders_array;
        private static Dictionary<string, object> countries;
        private static Dictionary<string, object> zones;
        private static Dictionary<string, object> resp;
        private static Dictionary<string, object> response;
        //private static Dictionary<string, object> currencies;
        private static Dictionary<object, object> configuration;
        private static Dictionary<string, object> renewal_order; //= new Dictionary<string, object>();
        private static Dictionary<string, object> renewal_order_product;  //=new Dictionary<string, object>();
        private static Dictionary<string, object> renewal_order_status_history;
        private static Dictionary<string, object> renewal_order_total;
        private static Dictionary<string, object> renewal_order_subtotal;
        //private static Dictionary<string, object> fulfillment_batch_week;
        private static Dictionary<string, object> sql_data_array;
        private static Dictionary<string, object> sql_data_array2;
        private static Dictionary<string, object> transaction;
        private static List<string> orders_columns;
        private static List<string> orders_products_columns;
        //private static Dictionary<string, object> renewals_billing_series_array;
        //private static Dictionary<string, object> skinsites;
        private static DataTable renewals_billing_series_array;
        private static DataTable skinsites;
        private static DataTable skinsites_configuration_defines;
        private static DataTable currencies;
        private static DataTable config_values;
        private static DataTable getFulfillmentBatchWeek = null;
        private static DataTable fulfillment_delay_batch_week_array;
        private static DataTable fulfillment_current_batch_week_array;
        /// <summary>
        /// 
        /// </summary>
        //DatabaseTables dt = new DatabaseTables();
        public static string connectionString =
            ConfigurationManager.ConnectionStrings["databaseConnectionString"].ToString();
        public static MySqlConnection myConn = new MySqlConnection(connectionString);
        private static MySqlCommand command;
        private static MySqlCommand command2;
        private static MySqlCommand command3;
        private static MySqlCommand command4;
        private static MySqlCommand command5;
        private static MySqlCommand command6;
        private static MySqlCommand command7;
        private static MySqlCommand command8;
        private static MySqlCommand command9;

        private static void Main(string[] args)
        {
            try
            {
                // Set our email body string for our e-mail to an empty string.
                string email_body = string.Empty;
                //this allows the script to run without any maximum executiont time.
                // set_time_limit(0)  Do we need this in .NET?;
                myConn.Open();
                set_all_defines();
                config_values = get_configuration_values();
                set_config_values(config_values);

                //set up logging of script to file
                Console.WriteLine("Begin renewal main" + "\n");
                log_renewal_process("Begin renewal main");
                email_body += "Begin renewal main \n\n";
                Console.WriteLine("Begin init_renewal_orders");
                log_renewal_process("Begin init_renewal_orders");
                number_of_renewal_orders_created = init_renewal_orders();
                Console.WriteLine("End init_renewal_orders. number of renewal orders created: " +
                                  number_of_renewal_orders_created.ToString() + "\n");
                log_renewal_process("End init_renewal_orders. number of renewal orders created: " + number_of_renewal_orders_created.ToString());
                email_body += "End init_renewal_orders. number of renewal orders created: " +
                              number_of_renewal_orders_created.ToString() + "\n\n";
                //let"s charge first since if it fails we can create the 1015 right after this.
                Console.WriteLine("Begin charging renewal orders");
                log_renewal_process("Begin charging renewal orders");
                number_of_renewal_orders_charged = charge_renewal_orders();
                Console.WriteLine("End charging renewal orders. number of renewal orders charged: " +
                                  number_of_renewal_orders_charged.ToString() + "\n");
                log_renewal_process("End charging renewal orders. number of renewal orders charged: " + number_of_renewal_orders_charged.ToString());
                email_body += "End charging renewal orders. number of renewal orders charged: " +
                              number_of_renewal_orders_charged.ToString() + "\n\n";
                Console.WriteLine("Begin creating renewal invoices");
                log_renewal_process("Begin creating renewal invoices");
                number_of_renewal_invoices_created = create_first_effort_renewal_invoices();
                Console.WriteLine("End creating renewal invoices. number of renewal invoices created: " +
                                  number_of_renewal_invoices_created.ToString() + "\n");
                log_renewal_process("End creating renewal invoices. number of renewal invoices created: " + number_of_renewal_invoices_created.ToString());
                email_body += "End creating renewal invoices. number of renewal invoices created: " +
                              number_of_renewal_invoices_created.ToString() + "\n\n";
                Console.WriteLine("Begin creating additional renewal invoices");
                log_renewal_process("Begin creating additional renewal invoices");
                number_of_additional_renewal_invoices_created = create_additional_renewal_invoices();
                Console.WriteLine(
                    "End creating additional renewal invoices. number of additional renewal invoices created: " +
                    number_of_additional_renewal_invoices_created.ToString() + "\n");
                //log_renewal_process("End creating additional renewal invoices. number of additional renewal invoices created: " + number_of_additional_renewal_invoices_created.ToString());
                email_body +=
                    "End creating additional renewal invoices. number of additional renewal invoices created: " +
                    number_of_additional_renewal_invoices_created.ToString() + "\n\n";
                Console.WriteLine("Begin sending renewal email invoices");
                log_renewal_process("Begin sending renewal email invoices");
                number_of_renewal_email_invoices_sent = send_renewal_email_invoices();
                Console.WriteLine("End sending renewal email invoices. number of renewal email invoices sent: " +
                                  number_of_renewal_email_invoices_sent.ToString() + "\n");
                log_renewal_process("End sending renewal email invoices. number of renewal email invoices sent: " + number_of_renewal_email_invoices_sent.ToString());
                email_body += "End sending renewal email invoices. number of renewal email invoices sent: " +
                              number_of_renewal_email_invoices_sent.ToString() + "\n\n";
                Console.WriteLine("Begin creating renewal paper invoices file");
                log_renewal_process("Begin creating renewal paper invoices file");
                number_of_renewal_paper_invoices_file_records = create_renewal_paper_invoices_file();
                Console.WriteLine(
                    "End creating renewal paper invoices file. number of renewal paper invoices file records: " +
                    number_of_renewal_paper_invoices_file_records.ToString() + "\n");
                log_renewal_process("End creating renewal paper invoices file. number of renewal paper invoices file records: " + number_of_renewal_paper_invoices_file_records.ToString());
                email_body +=
                    "End creating renewal paper invoices file. number of renewal paper invoices file records: " +
                    number_of_renewal_paper_invoices_file_records.ToString() + "\n\n";
                Console.WriteLine("Begin cleaning up renewal invoices");
                log_renewal_process("Begin cleaning up renewal invoices");
                number_of_invoices_cleaned_up = clean_up_renewal_invoices();
                Console.WriteLine("End cleaning up renewal invoices. number of renewal invoices cleaned up: " +
                                  number_of_invoices_cleaned_up.ToString() + "\n");
                log_renewal_process("End cleaning up renewal invoices. number of renewal invoices cleaned up: " + number_of_invoices_cleaned_up.ToString());
                email_body += "End cleaning up renewal invoices. number of renewal invoices cleaned up: " +
                              number_of_invoices_cleaned_up.ToString() + "\n\n";
                //now see if we need to cancel any renewal orders.
                Console.WriteLine("Begin mass cancelling renewal orders");
                log_renewal_process("Begin mass cancelling renewal orders");
                number_of_renewal_orders_mass_cancelled = mass_cancel_renewal_orders();
                Console.WriteLine("End mass cancelling renewal orders. number of renewal orders mass cancelled: " +
                                  number_of_renewal_orders_mass_cancelled.ToString() + "\n");
                log_renewal_process("End mass cancelling renewal orders. number of renewal orders mass cancelled: " + number_of_renewal_orders_mass_cancelled.ToString());
                email_body += "End mass cancelling renewal orders. number of renewal orders mass cancelled: " +
                              number_of_renewal_orders_mass_cancelled.ToString() + "\n\n";

                Console.WriteLine("End renewal main");
                log_renewal_process("End renewal main");
                email_body += "End renewal main \n\n";

                // Send e-mail saying we have completed the renewal run.
                //tep_mail("M2 Media Group Jobs", "jobs@m2mediagroup.com", "Renewal Process Successful", $email_body, "BlueDolphin", "jobs@m2mediagroup.com", "", "",false);
                //tep_mail("Michael Borchetta", "mborchetta@m2mediagroup.com", "Renewal Process Successful", $email_body, "BlueDolphin", "jobs@m2mediagroup.com", "", "",false);
                //tep_mail("Martin Schmidt", "mschmidt@mcswebsolutions.com", "Renewal Process Successful", $email_body, "BlueDolphin", "jobs@m2mediagroup.com", "", "",false);

                myConn.Close();
                Console.ReadLine();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static int init_renewal_orders()
        {
            try
            {
                //there was a problem with products_quantity being in both orders_products and products,
                //with 2 different meanings. So we just pick what we need from product and get the rest

                //select all orders that have a continuous_service, with no renewal invoices created,
                // user want to renew (auto_renew), paid orders and renewal notice < today.

                string create_renewal_orders_query_string = @"
		        SELECT
			        o.`renewal_payment_cards_id`,
			        pc.cc_type AS renewal_cc_type,
			        pc.cc_number AS renewal_cc_number,
			        pc.cc_number_display AS renewal_cc_number_display,
			        pc.cc_expires AS renewal_cc_expires,
			        pc.cc_owner AS renewal_cc_owner,
			        o.*, op.*, s.*,
			        p.continuous_service,
			        p.products_status
		        FROM
			        orders o LEFT JOIN payment_cards pc ON (pc.payment_cards_id = o.renewal_payment_cards_id),
			        orders_products op,
			        products p,
			        skus s
		        WHERE
			        o.orders_id = op.orders_id
			        AND op.products_id = p.products_id
			        AND op.skus_id = s.skus_id
			        AND p.continuous_service = 1
			        AND o.auto_renew = 1
			        AND o.renewal_error != 1
			        AND o.orders_status = 2
			        AND o.renewal_date is not null
			        AND to_days(o.renewal_date) > to_days(DATE_SUB(curdate(),INTERVAL 60 DAY))
			        AND to_days(o.renewal_date) <= to_days(curdate())
	        ";
                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = create_renewal_orders_query_string;
                command.ExecuteNonQuery();
                MySqlDataReader myReader;
                myReader = command.ExecuteReader();
                int num_orders = 0;

                while (myReader.Read())
                {
                    num_orders++;
                }

                myReader.Close();
                myReader = command.ExecuteReader();

                if (Debug)
                    Console.WriteLine("number of orders to be examined: " +
                                      num_orders.ToString() + "\n");

                while (myReader.Read())
                {
                    original_order_products_id = Convert.ToInt32(myReader["products_id"]);
                    original_order_skus_id = Convert.ToInt32(myReader["skus_id"]);
                    original_order_skus_type = myReader["skus_type"].ToString();
                    original_order_skus_type_order = Convert.ToInt32(myReader["skus_type_order"]);
                    original_order_skus_type_order_period = Convert.ToInt32(myReader["skus_type_order_period"]);
                    original_order_id = Convert.ToInt32(myReader["orders_id"]);
                    original_products_status = Convert.ToInt32(myReader["products_status"]);
                    // Is it a postcard confirmation?
                    original_is_postcard_confirmation = Convert.ToInt32(myReader["is_postcard_confirmation"]);

                    //if the original sku was an intro sku then use the 1st RENEW sku type_order_period for the same
                    //skus_type_order

                    if (original_order_skus_type.ToString() == "INTRO")
                    {
                        renewal_skus_type_order_period = "1";
                    }
                    else
                    {
                        //add 1 to the renewal skus type order year to get the next renewal sku in line for the same
                        //skus_type_order.
                        renewal_skus_type_order_period =
                            (Convert.ToInt32(original_order_skus_type_order_period) + 1).ToString();
                    }

                    if (Debug)
                        Console.WriteLine("order number: " + original_order_id.ToString() +
                                          " renewal_skus_type_order_period: " + renewal_skus_type_order_period + "\n");

                    //check to make sure that the renewal skus is available and if it is is active.
                    //if the sku isn"t available and the original sku was an INTRO sku no renewals are possible.
                    //if the original sku was a "RENEW" sku and it isn"t available go back to the original renewal sku
                    //and check to see if that one is available, if that isn"t available no renewal is possible.
                    //we check for the s.skus_type_order = $original_order_skus_type_order since we might be renewing
                    //a 2 year pub we need to use the 2 year pub renewal skus for that.

                    string potential_renewal_skus_query_string = @"
			    select
				    *,
				    if(p.first_issue_delay_days=0,pf.first_issue_delay_days, p.first_issue_delay_days) as first_issue_delay_days
			    from
				    skus s,
				    products 
				    publication_frequency pf
			    where
				    s.products_id = '" + original_order_products_id.ToString() + @"'
				    and s.skus_type = 'RENEW'
				    and s.skus_type_order = '" + original_order_skus_type_order.ToString() + @"'
				    and s.skus_status = 1
				    and s.fulfillment_flag = 1
				    and s.products_id = p.products_id
				    and p.publication_frequency_id = pf.publication_frequency_id
			    order by
				    s.skus_type_order_period desc
		    ";

                    command2 = new MySqlCommand(potential_renewal_skus_query_string, myConn);
                    command2.ExecuteNonQuery();

                    // Potential Renewal SKUs found: now find the right one

                    int previous_orders_skus_type_order_period = original_order_skus_type_order_period - 1;

                    MySqlDataReader myReader2;
                    myReader2 = command2.ExecuteReader();
                    int num_skus = 0;

                    while (myReader2.Read())
                    {
                        num_skus++;
                    }

                    myReader2.Close();
                    myReader2 = command2.ExecuteReader();

                    if (Debug)
                        Console.Write("number of renewal skus for product : " + original_order_products_id.ToString() +
                                      ": " + num_skus.ToString() + "\n");

                    // START MCS MOD FOR RECORDING REASON FOR FAILED POTENTIAL SKU SEARCH (4/30/2012)
                    if (!myReader2.HasRows)
                    // No renewal SKUs could be used for this order; record reason why and move on to next order.
                    {
                        // Were there no renewal SKUs at all?
                        string ANY_potential_renewal_skus_query_string = "select * from skus where products_id = '" +
                                                                         original_order_products_id +
                                                                         "' and skus_type = 'RENEW'";
                        command3 = new MySqlCommand(ANY_potential_renewal_skus_query_string, myConn);
                        command3.ExecuteNonQuery();

                        MySqlDataReader noSku;
                        noSku = command3.ExecuteReader();

                        if (!noSku.HasRows)
                        {
                            ANY_potential_renewal_skus_query_string = "update " + TABLE_ORDERS +
                                                                      " set renewal_error='1', renewal_error_description='Error: no renewal SKU exists for the PRODUCT in this order.' where orders_id= " +
                                                                      original_order_id.ToString();

                            command4 = new MySqlCommand(ANY_potential_renewal_skus_query_string, myConn);
                            command4.ExecuteNonQuery();
                        }
                        else // Of the potential renewal SKUs, were there none for this order's SKUS_TYPE_ORDER?
                        {
                            string potential_renewal_skus_for_SKUS_TYPE_ORDER_query_string =
                                "select * from skus where products_id = '" + original_order_products_id.ToString() +
                                "' and skus_type = 'RENEW' and skus_type_order='" +
                                original_order_skus_type_order.ToString() + "'";
                            command4 = new MySqlCommand(potential_renewal_skus_for_SKUS_TYPE_ORDER_query_string, myConn);
                            command4.ExecuteNonQuery();

                            MySqlDataReader yesSku;
                            yesSku = command4.ExecuteReader();

                            if (!yesSku.HasRows)
                            {
                                string NoYesSku = "update " + TABLE_ORDERS +
                                                  " set renewal_error='1', renewal_error_description='Error: No renewal SKU(s) with the proper SKU TYPE ORDER (" +
                                                  original_order_skus_type_order.ToString() +
                                                  ") could be found for this order.' where orders_id=" +
                                                  original_order_id.ToString();
                                command5 = new MySqlCommand(NoYesSku, myConn);
                                command5.ExecuteNonQuery();

                            }
                            else // If there are potential renewal SKUs with this order's skus_type_order, are none ACTIVE?
                            {
                                string potential_ACTIVE_renewal_skus_query_string =
                                    "select * from skus where products_id = '" + original_order_products_id.ToString() +
                                    "' and skus_type = 'RENEW' and skus_type_order='" +
                                    original_order_skus_type_order.ToString() + "' and skus_status='1'";
                                command5 = new MySqlCommand(potential_ACTIVE_renewal_skus_query_string, myConn);
                                command5.ExecuteNonQuery();

                                MySqlDataReader activeSku;
                                activeSku = command5.ExecuteReader();

                                if (!activeSku.HasRows)
                                {
                                    command6 =
                                        new MySqlCommand(
                                            "update " + TABLE_ORDERS +
                                            " set renewal_error='1', renewal_error_description='Error: No ACTIVE renewal SKU(s) could be found for this order.' where orders_id=" +
                                            original_order_id.ToString(), myConn);
                                    command6.ExecuteNonQuery();
                                }
                                else
                                {
                                    command6 =
                                        new MySqlCommand(
                                            "update " + TABLE_ORDERS +
                                            " set renewal_error='1', renewal_error_description='Error: An UNKNOWN error has occured while searching for a renewal SKU for this order; please contact the administrator.' where orders_id='" +
                                            original_order_id.ToString() + "'", myConn);
                                    command6.ExecuteNonQuery();
                                }
                                activeSku.Close();
                            }
                            yesSku.Close();
                        }
                        noSku.Close();
                        continue;
                    } // END MCS MOD FOR RECORDING REASON FOR FAILED POTENTIAL SKU SEARCH

                    Dictionary<string, object> renewal_sku = new Dictionary<string, object>();
                    renewal_sku = null;

                    // Potential Renewal SKUs found: now find the right one
                    while (myReader2.Read())
                    {
                        //since we go descending through the type orders year, we can look first for the renewal type order year,
                        //then the original type order year, and if that doesn't exist, keep going until we find one.
                        //if renewal_sku isn't populated at the end of this loop, it means we don't have any skus
                        //for renewal.

                        //First check for the next renewal Sku.
                        if (Convert.ToInt32(myReader2["skus_type_order_period"]) ==
                            Convert.ToInt32(renewal_skus_type_order_period))
                        {
                            for (int lp = 0; lp < myReader2.FieldCount; lp++)
                            {
                                renewal_sku.Add(myReader2.GetName(lp), myReader2.GetValue(lp));
                            }
                            break;
                        }

                        //Second check for the original renewal skus type order
                        if (Convert.ToInt32(myReader2["skus_type_order_period"]) ==
                            original_order_skus_type_order_period)
                        {
                            for (int lp = 0; lp < myReader2.FieldCount; lp++)
                            {
                                renewal_sku.Add(myReader2.GetName(lp), myReader2.GetValue(lp));
                            }
                            break;
                        }
                        //now if we have already passed the original skus type order then go back to any previous ones.
                        if (Convert.ToInt32(myReader2["skus_type_order_period"]) < original_order_skus_type_order_period)
                        {
                            //only do this for type_order_period 1 and up.
                            if (previous_orders_skus_type_order_period > 0)
                            {
                                //check to see if there are any previous type order, if not, move on and see if there are
                                //are any previous to that.
                                if (Convert.ToInt32(myReader2["skus_type_order_period"]) ==
                                    previous_orders_skus_type_order_period)
                                {

                                    for (int lp = 0; lp < myReader2.FieldCount; lp++)
                                    {
                                        renewal_sku.Add(myReader2.GetName(lp), myReader2.GetValue(lp));
                                    }
                                    break;
                                }
                                else
                                {
                                    previous_orders_skus_type_order_period--;
                                }
                            }
                        }
                    }

                    if (Debug)
                    {
                        Console.WriteLine("Trying to find renewal skus  : " + renewal_skus_type_order_period + ": ");
                        //debug($renewal_sku, 'renewal_sku');
                    }

                    //at this point we know there isn't any valid renewal sku, so move on to the next order.
                    if (renewal_sku.Count == 0 || renewal_sku == null)
                    {
                        //restored error_description
                        string update_sql = "update " + TABLE_ORDERS +
                                            " set renewal_error='1', renewal_error_description='Error: renewal sku with proper sku type order period (1 to " +
                                            renewal_skus_type_order_period +
                                            ") does not exist for this order' where orders_id='" +
                                            original_order_id.ToString() + "'";

                        command3 = new MySqlCommand(update_sql, myConn);
                        command3.ExecuteNonQuery();

                        if (Debug)
                            Console.WriteLine("no renewal sku found\n");

                        continue;
                    }

                    myReader2.Close();

                    //now that we have a valid sku lets put the order in the right track.
                    is_perfect_renewal = false;
                    orders_array = new Dictionary<string, object>();

                    for (int oa = 0; oa < myReader.FieldCount; oa++)
                    {
                        orders_array.Add(myReader.GetName(oa), myReader.GetValue(oa));
                    }

                    if (isPerfectRenewal(orders_array))
                    {
                        renewals_billing_series_id = Convert.ToInt32(TRACK1);
                        is_perfect_renewal = true;
                    }
                    else
                    {
                        if (orders_array["cc_number"].ToString() == string.Empty &&
                            orders_array["renewal_cc_number"].ToString() == string.Empty)
                        {
                            renewals_billing_series_id = Convert.ToInt32(TRACK2_CHECK);
                        }
                        else if (orders_array["is_postcard_confirmation"].ToString() == "1")
                        {
                            renewals_billing_series_id = Convert.ToInt32(TRACK2_PC);
                        }
                        else
                        {
                            renewals_billing_series_id = Convert.ToInt32(TRACK2_BAD_CC);
                        }
                    }

                    if (Debug)
                        Console.WriteLine("Perfect Renewal: " + is_perfect_renewal.ToString() + "; track chosen: " +
                                          renewals_billing_series_id.ToString() + "\n");

                    //create the order and if succeeded, we will update the original order with renewal info and also so it won't be pulled again for renewals.
                    renewal_orders_id = 0;
                    renewal_orders_id = create_renewal_order(orders_array, renewals_billing_series_id,
                        is_perfect_renewal, renewal_sku, is_postcard_confirmation);

                    if (renewal_orders_id != 0)
                    {
                        // Update our original order setting the renewal_date to null to prevent the order from being picked up again and
                        // a duplicate renewal being created. Also set renewal_orders_id so we can associate the original and renewal orders.

                        command4 = new MySqlCommand(@"update orders
						  set renewal_orders_id = '" + renewal_orders_id.ToString() +
                                                    "', renewal_date = null, renewal_payment_cards_id = '' where orders_id = '" +
                                                    original_order_id.ToString() + "'",
                            myConn);
                        command4.ExecuteNonQuery();

                        if (Debug)
                            Console.WriteLine("Created renewal order\n");
                    }

                    if (Debug)
                        Console.WriteLine("\n\n");

                    number_of_renew_invoices_prepared++;

                    renewal_sku.Clear();
                    orders_array.Clear();
                }
                myReader.Close();
                return number_of_renew_invoices_prepared;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static int charge_renewal_orders()
        {
            try
            {
                  is_gc_order = true;
                /*  
	                //for windows we need to use the functions. On Unix this is compiled in.
	                if (IS_UNIX_ENVIRONMENT == 'false') {
		                include('php_pfpro.php');
	                }*/

                //make sure we have sent them renewalinvoices
                //eg IF orderItem.renewal_invoices_sent > 0 THEN
                //
                //if the charge fails we need to put the user in track 2 1015
                //move all invoices from renewals_invoices to renewal_invoices_history with
                //comments: Changed from 1014 to 1015.
                //create new invoices based for 1015.
                //make sure to update the order"s renewals_billing_series_id to the new one.

                //make sure the order is still PENDING, they might have paid already.

                int number_of_renewal_charged = 0;

                string charge_renewal_orders_query_string = @"
		            select
			            sk.override_renewal_billing_descriptor,
			            o.*, op.*, s.*,
			            p.continuous_service,
			            p.products_status,
			            pd.products_billing_descriptor
		            from
			            orders o,
			            orders_products op,
			            products p,
			            products_description pd,
			            skus s,
			            skinsites sk
		            where
			            o.orders_id = op.orders_id
			            and sk.skinsites_id = o.skinsites_id
			            and op.products_id = p.products_id
			            and op.skus_id = s.skus_id
			            and o.is_renewal_order = 1
			            and o.renewal_transaction_date is not null
			            and o.renewal_error != 1
			            and to_days(o.renewal_transaction_date) > to_days(DATE_SUB(curdate(),INTERVAL 30 DAY))
			            and to_days(o.renewal_transaction_date) <= to_days(curdate())
			            and pd.products_id = op.products_id
	            ";

                command = new MySqlCommand(charge_renewal_orders_query_string, myConn);
                command.ExecuteNonQuery();
                MySqlDataReader myReader;
                myReader = command.ExecuteReader();

                while (myReader.Read())
                {
                    customers_id = Convert.ToInt32(myReader["customers_id"]);
                    orders_id = Convert.ToInt32(myReader["orders_id"]);
                    products_id = Convert.ToInt32(myReader["products_id"]);
                    skus_type_order = Convert.ToInt32(myReader["skus_type_order"]);
                    prior_orders_id = Convert.ToInt32(myReader["prior_orders_id"]);
                    renewal_order_status = Convert.ToInt32(myReader["orders_status"]);
                    skus_status = Convert.ToInt32(myReader["skus_status"]);
                    continuous_service = Convert.ToInt32(myReader["continuous_service"]);
                    auto_renew = Convert.ToInt32(myReader["auto_renew"]);
                    renewal_orders_id = Convert.ToInt32(myReader["orders_id"]);
                    renewal_invoices_created = Convert.ToInt32(myReader["renewal_invoices_created"]);
                    renewal_invoices_sent = Convert.ToInt32(myReader["renewal_invoices_sent"]);
                    final_price = Convert.ToDouble(myReader["final_price"]);
                    cc_number = myReader["cc_number"].ToString();
                    cc_expires = myReader["cc_expires"].ToString();
                    email_address = myReader["customers_email_address"].ToString();
                    billing_first_name = myReader["billing_first_name"].ToString();
                    billing_last_name = myReader["billing_last_name"].ToString();
                    billing_address = myReader["billing_street_address"].ToString();
                    billing_city = myReader["billing_city"].ToString();
                    billing_state = myReader["billing_state"].ToString();
                    billing_postcode = myReader["billing_postcode"].ToString();
                    billing_country = myReader["billing_country"].ToString();
                    renewals_credit_card_charge_attempts =
                        Convert.ToInt32(myReader["renewals_credit_card_charge_attempts"]) + 1;
                    renewals_expiration_date_failures = Convert.ToInt32(myReader["renewals_expiration_date_failures"]);
                    is_postcard_confirmation = Convert.ToInt32(myReader["is_postcard_confirmation"]);
                    payment_cards_id = Convert.ToInt32(myReader["payment_cards_id"]);
                    skinsites_id = Convert.ToInt32(myReader["skinsites_id"]);
                    override_renewal_billing_descriptor = myReader["override_renewal_billing_descriptor"].ToString();
                    products_billing_descriptor = myReader["products_billing_descriptor"].ToString();

                    // echo "Products Billing Descriptor Override: $override_renewal_billing_descriptor\n";

                    //if the expiration date is expired, we should increase by 1 year.
                    cc_expires_year = cc_expires.Substring(2, 2);
                    cc_expires_month = cc_expires.Substring(0, 2);

                    int n;
                    bool isNumeric = int.TryParse(cc_expires_year, out n);

                    // Check to make sure our month and year are numeric.
                    if (!isNumeric)
                    {
                        cc_expires_year = DateTime.Now.Year.ToString();
                    }

                    isNumeric = int.TryParse(cc_expires_month, out n);
                    if (!isNumeric)
                    {
                        cc_expires_month = DateTime.Now.Month.ToString();
                    }

                    // Check if our month is less than one. If so than make it 01.
                    if (Convert.ToInt32(cc_expires_month) < 1)
                    {
                        cc_expires_month = "01";
                    }

                    // Check if our month is greater than twelve. If so than make it 12.
                    if (Convert.ToInt32(cc_expires_month) > 12)
                    {
                        cc_expires_month = "12";
                    }

                    /* FORMER DATE MODS. Replaced in September 2012 (MCS)
				// Check to see if our year is less than our current year. If so set to our current year.
				if ($cc_expires_year < date('y')) {
					$cc_expires_year = date('y');
				}

				// Check to see if our year is 10 greater than our current year. If so set to our current year.
				if ($cc_expires_year > (date('y') + 10))
				{
					$cc_expires_year = date('y');
				}

				// Finally if it is our current year and our month is less than our current month than add one to our year.
				if (($cc_expires_year == date('y')) && ($cc_expires_month < date('m'))) {
					$cc_expires_year++;
				}

				// Add one less then our current attempt to the expiration year.
				// We do this to increment our expiuration year by one for each attempt
				// accept for our first attempt. We will not store this expiration date
				// unless we succeed in charging the consumer.
				$cc_expires_year += ($renewals_credit_card_charge_attempts - 1);
		// END FORMER DATE MODS.*/

                    /* NEW DATE MODS. September 2012 (MCS) */
                    // If original expiration date failed in first charge attempt
                    if (renewals_expiration_date_failures == 1)
                    {
                        if (is_date_stale(cc_expires_month, cc_expires_year))
                        {
                            cc_expires_year = (Convert.ToInt32(cc_expires_year) + 3).ToString();
                        }
                        if (is_date_stale(cc_expires_month, cc_expires_year))
                        {
                            cc_expires_year = DateTime.Now.Year.ToString();
                        }
                    }
                    // If modified expiration date failed in second charge attempt
                    if (renewals_expiration_date_failures == 2)
                    {
                        if (is_date_stale(cc_expires_month, cc_expires_year))
                        {
                            cc_expires_year = (Convert.ToInt32(cc_expires_year) + 2).ToString();
                        }
                        if (is_date_stale(cc_expires_month, cc_expires_year))
                        {
                            cc_expires_year = (DateTime.Now.Year + 1).ToString();
                        }
                    }
                    /* END NEW DATE MODS. */
                    // Make sure our month and year are two digits.
                    cc_expires_year = cc_expires_year.PadLeft(2, '0');
                    cc_expires_month = cc_expires_month.PadLeft(2, '0');
                    cc_expires = cc_expires_month + "" + cc_expires_year;

                    //check to see if the order is still valid for charging
                    check_renewal_order_result = check_renewal_order();
                    if (check_renewal_order_result != "true")
                    {
                        //Since this isn't a valid renewal order anylonger, we don't charge, set the charge_date = null
                        //so it won't get pulled again.
                        command2 = new MySqlCommand(@"update orders set renewal_transaction_date = null where orders_id = '" + renewal_orders_id.ToString() + "'", myConn);
                        command2.ExecuteNonQuery();
                        log_renewal_process("charge_renewal_orders(): Not charging this order, because " + check_renewal_order_result, renewal_orders_id);
                        continue;
                    }

                    if (renewal_invoices_created == 0 || renewal_invoices_sent == 0)
                    {
                        log_renewal_process("charge_renewal_orders(): Not charging this order, because the renewal_invoices_created was " + renewal_invoices_created.ToString() + " and renewal_invoices_sent is " + renewal_invoices_sent.ToString() + ". Both need to be 1!", renewal_orders_id);
                        continue;
                    }

                    //charge the card

                    //start by entering an new (empty) cc_transaction record to get
                    //a transaction_id that will be stored on the order record(s)
                    //we will update this table in the after_process().
                    sql_data_array = new Dictionary<string, object>();
                    sql_data_array["cc_reference_id"] = "";
                    sql_data_array["cc_auth_code"] = "";
                    sql_data_array["cc_transactions_date"] = "now()";
                    string transactions_query = tep_db_perform(TABLE_CC_TRANSACTIONS, sql_data_array);
                    command3 = new MySqlCommand(transactions_query, myConn);
                    command3.ExecuteNonQuery();
                    int cc_transactions_id = Convert.ToInt32(command.LastInsertedId);

                    //Add this cc_transaction to this order.
                    command4 = new MySqlCommand(@"INSERT INTO " + TABLE_CC_TRANSACTIONS_ORDERS + " (cc_transactions_id, orders_id) VALUES ('" + cc_transactions_id.ToString() + "', '" + orders_id.ToString() + "')", myConn);
                    command4.ExecuteNonQuery();

                    Dictionary<string, object> countries_array = new Dictionary<string, object>();
                    countries_array = countries;
                    string billing_country_name = countries_array[billing_country].ToString();
                    string is_gc_order_returned = (is_gc_order == true) ? "True" : "False";

                    transaction = new Dictionary<string, object>();

                    transaction["USER"] = MODULE_PAYMENT_PAYFLOWPRO_USER.Trim();
                    transaction["VENDOR"] = MODULE_PAYMENT_PAYFLOWPRO_VENDOR.Trim();
                    transaction["PARTNER"] = MODULE_PAYMENT_PAYFLOWPRO_PARTNER.Trim();
                    transaction["PWD"] = get_pfp_pwd().Trim();
                    transaction["TRXTYPE"] = MODULE_PAYMENT_PAYFLOWPRO_TRXTYPE.Trim();
                    transaction["TENDER"] = MODULE_PAYMENT_PAYFLOWPRO_TENDER.Trim();
                    transaction["AMT"] = final_price.ToString("#,##0.00");
                    transaction["ACCT"] = decrypt_cc(Convert.ToInt32(cc_number), customers_id).Substring(0, 19);
                    transaction["EXPDATE"] = cc_expires;
                    transaction["FREIGHTAMT"] = "";
                    transaction["TAXAMT"] = "";
                    transaction["FIRSTNAME"] = billing_first_name;
                    transaction["LASTNAME"] = billing_last_name;
                    transaction["STREET"] = billing_address;
                    transaction["CITY"] = billing_city;
                    transaction["STATE"] = billing_state;
                    transaction["ZIP"] = billing_postcode;
                    transaction["COUNTRY"] = billing_country_name;
                    transaction["EMAIL"] = email_address;
                    transaction["IS_GC_ORDER"] = is_gc_order_returned;
                    transaction["SHIPTOFIRSTNAME"] = "";
                    transaction["SHIPTOLASTNAME"] = "";
                    transaction["SHIPTOSTREET"] = "";
                    transaction["SHIPTOCITY"] = "";
                    transaction["SHIPTOSTATE"] = "";
                    transaction["SHIPTOZIP"] = "";
                    transaction["CVV2"] = "";
                    transaction["COMMENT1"] = cc_transactions_id;
                    transaction["ORDERSOURCE"] = "Recurring";
                    transaction["CCTRANSACTIONID"] = cc_transactions_id;
                    transaction["REPORTGROUP"] = get_merchant_processor_reporting_group(skinsites_id);
                    transaction["L_BDESCRIP_OVERRIDE"] = override_renewal_billing_descriptor;

                    //setup the line items specific to First Data
                    suffix = "1";
                    transaction["L_QTY" + suffix] = myReader["products_quantity"];
                    transaction["L_COMMCODE" + suffix] = "";
                    transaction["L_DESC" + suffix] = myReader["products_name"];
                    transaction["L_BDESCRIP" + suffix] = myReader["products_billing_descriptor"];
                    transaction["L_UOM" + suffix] = "";
                    transaction["L_COST" + suffix] = Convert.ToDouble(myReader["products_price"]).ToString("#,##0.00");
                    transaction["L_PRODCODE" + suffix] = myReader["products_model"];
                    transaction["L_DISCOUNT" + suffix] = "";
                    transaction["L_AMT" + suffix] = final_price.ToString("#,##0.00");
                    transaction["L_TAXAMT" + suffix] = Convert.ToDouble(myReader["products_tax"]).ToString("#,##0.00");

                    debug(transaction, "tansaction");
                    Environment.SetEnvironmentVariable("PFPRO_CERT_PATH", MODULE_PAYMENT_PAYFLOWPRO_PFPRO_CERT_PATH_ENV, EnvironmentVariableTarget.Machine);
                    response = new Dictionary<string, object>();
                    response = pfpro_process(transaction, MODULE_PAYMENT_PAYFLOWPRO_HOSTADDRESS);
                    if (Debug == true)
                    {
                        debug(response, "payflowresponse");
                    }

                    // Up our counter for renewal_credit_card_charge_attempts by one.
                    command5 = new MySqlCommand("update orders set renewals_credit_card_charge_attempts = " + renewals_credit_card_charge_attempts.ToString() + " where orders_id = '" + renewal_orders_id.ToString() + "'",myConn);
                    command5.ExecuteNonQuery();

                    // if we don't get a response the card might have been charged, need to check with Merchant Processor.
                    // if there was a good transaction, we will update the order status to 'Paid' and add the cc_reference_id to
                    // the transaction id from Merchant Processor, also amount_paid will be set and amount_owed will be set to 0.
                    // If there was no transaction or a bad one, we will update the order status to 'Cancel' since amount_paid wasn't updated
                    // it will not show up in the refund report.
                    //

                    if (response["AUTHCODE"] != string.Empty)
                    {
                        authCode = response["AUTHCODE"].ToString();
                    }
                    else
                    {
                        authCode = string.Empty;
                    }

                    if (response == null)
                    {
                        the_code = response["RESULT"].ToString();

                        //do nothing need to check with Merchant Processor and see if the cc was charged or not.
                        //Order stays in pending.

                        //update the order so it won't get pulled again.
                        command5 = new MySqlCommand("update orders set renewal_transaction_date = null where orders_id = '" + orders_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                    }
                    else if (response["RESULT"].ToString() != "0")
                    {
                        //if there was  a response but it wasn't 0. Orders are set to Pending status, this is
                        //different from the live site, since we are not pushing this order into track to
                        //it needs to still Pending.
                        //update the orders' information. we are not using tep_is_valid_status_change
                        //because we know it is a valid change. We don't want admin to change Pend->Error.

                        comments = MODULE_PAYMENT_PAYFLOWPRO_TEXT_ERROR + " " + response["RESPMSG"].ToString();
                        comments = comments.Substring(0, 255);

                        command5 = new MySqlCommand("update " + TABLE_CC_TRANSACTIONS + " set cc_reference_id = '" + response["PNREF"].ToString() + "', cc_auth_code = '" + authCode + "', cc_transactions_message= '" + response["RESULT"].ToString() + ":" + response["RESPMSG"].ToString() + "' where cc_transactions_id = '" + cc_transactions_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();

                        //check for the certain response codes, if it finds it push on through the recycling process
                        the_code = response["RESULT"].ToString();
                        the_code = the_code.Substring(0, 3);

                        if (the_code == "501" || the_code == "302" || the_code == "301" || the_code == "328" || the_code == "327" || the_code == "311" || the_code == "326" || the_code == "355" || the_code == "610" || the_code == "612" || the_code == "611" || the_code == "714" || the_code == "716" || the_code == "321" || the_code == "330" || the_code == "324" || the_code == "310" || the_code == "322" || the_code == "304" || the_code == "323" || the_code == "303" || the_code == "307" || the_code == "325" || the_code == "140" || the_code == "713")
                        {
                            renewals_credit_card_charge_attempts = MAX_RENEWAL_CREDIT_CARD_CHARGE_ATTEMPTS;
                        }
                        // If there is an expiration date issue (added September 2012 - MCS):
                        if (the_code == "305")
                        {
                            renewals_expiration_date_failures++;
                            command5 = new MySqlCommand("update orders set renewals_expiration_date_failures = " + renewals_expiration_date_failures.ToString() + " where orders_id = '" + orders_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }

                        // If this was our last attempt than push into the next track otherwise queue it up for another try in the number of days specified by DEFAULT_RETRY_RENEWAL_CHARGE_DAYS in config.
                        // Last attempt is defined if the max renewal cc charge attempts or the max expiration failures is reached, both defined in config.
                        if (renewals_credit_card_charge_attempts >= MAX_RENEWAL_CREDIT_CARD_CHARGE_ATTEMPTS || renewals_expiration_date_failures >= MAX_RENEWAL_CREDIT_CARD_CHARGE_EXPIRATION_FAILURES)
                        {
                            if (is_postcard_confirmation == 1)
                            {// IS A POSTCARD CONFIRMATION

                                //Change the billing series from 1014 to 1018.
                                //first set the old billing series to in_progress=0 so these will be cleaned up, and set comments.
                                command5 = new MySqlCommand("update renewals_invoices set in_progress = 0, comments = 'Charging was not successful, putting the order in postcard track 2 (was 1014 now 1018)' where orders_id = '" + orders_id.ToString() + "'", myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();

                                //next change the order's renewal_billing_series to 1018, renewal_invoices_created and renewal_invoices_sent to 0. This will
                                //this will make sure the next time the orders get pulled for the next series.
                                command5 = new MySqlCommand("update orders set renewals_billing_series_id = '" + TRACK2_PC + "', renewal_invoices_sent = 0, renewal_invoices_created=0 where orders_id = '" + orders_id.ToString() + "'", myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();

                                //next change the order's renewal_billing_series to 1015, renewal_invoices_created and renewal_invoices_sent to 0. This will
                                //this will make sure the next time the orders get pulled for the next series.
                                command5 = new MySqlCommand("update orders set renewals_billing_series_id = '" + TRACK2_BAD_CC + "', renewal_invoices_sent = 0, renewal_invoices_created=0 where orders_id = '" + orders_id.ToString() + "'", myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();
                            }
                            else // IS NOT A POSTCARD CONFIRMATION
                            {

                                //Changing the billing series from 1014 to 1015.
                                //first set the old billing series to in_progress=0 so these will be cleaned up. and set comments.
                                command5 = new MySqlCommand("update renewals_invoices set in_progress = 0, comments = 'Charging was not successful, putting the order in track 2 (was 1014 now 1015)' where orders_id = '" + orders_id.ToString() + "'", myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();
                            }
                            //update the order so it won't get pulled again.
                            command5 = new MySqlCommand("update orders set renewal_transaction_date = null where orders_id = '" + orders_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                        else
                        {
                            // When setting the renewal_tranaction_date calculate as follows: Take the current date and time and
                            // add the value from the configuration key RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS.
                            // UPDATE 3/16/2012 (MCS): Removing logic to differentiate PostCards. All renewal_transaction_dates should use the DEFAULT_RETRY_RENEWAL_CHARGE_DAYS
                            /* OLD:
                                    if($is_postcard_confirmation){
                                        tep_db_query("update orders set renewal_transaction_date = date_add(curdate(), INTERVAL " . RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS . " DAY) where orders_id = '" . $renewal_orders_id . "'");
                                    }else{
                                        tep_db_query("update orders set renewal_transaction_date = date_add(renewal_transaction_date, INTERVAL " . DEFAULT_RETRY_RENEWAL_CHARGE_DAYS . " DAY) where orders_id = '" . $renewal_orders_id . "'");
                                    }
                            */
                            // NEW:
                            command5 = new MySqlCommand("update orders set renewal_transaction_date = date_add(renewal_transaction_date, INTERVAL " + DEFAULT_RETRY_RENEWAL_CHARGE_DAYS.ToString() + " DAY) where orders_id = '" + renewal_orders_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                    }
                    else
                    {
                        //On success update status, save the Response Message as part of the status change and
                        //update cc_transactions table.
                        //set the amount_owed to 0 and amount_paid to amount_owed.
                        comments = response["RESULT"].ToString() + ":"+ response["RESPMSG"].ToString();
                        comments = comments.Substring(0,255);
                        customer_notification = (SEND_EMAILS == "true") ? "1" : "0";

                        //enter the order into batch_item for fulfillment.
			            create_fulfillment_batch_item(orders_id, FULFILLMENT_FULFILL_ID);

                         command5 = new MySqlCommand("update " + TABLE_ORDERS + " set amount_paid = amount_owed, amount_owed = 0, orders_status = '" + MODULE_PAYMENT_PAYFLOWPRO_PFPRO_ORDER_STATUS_ID.ToString() + "' where orders_id = '" + orders_id.ToString() + "'", myConn);
                         command5.ExecuteNonQuery();
                         command5.Dispose();

                         command5 = new MySqlCommand("insert into " + TABLE_ORDERS_STATUS_HISTORY + " (orders_id, orders_status_id, date_added, customer_notified, comments) values ('" + orders_id.ToString() + "', '" + MODULE_PAYMENT_PAYFLOWPRO_PFPRO_ORDER_STATUS_ID.ToString() + "', now(), '" + customer_notification + "', '" + comments.Replace("'", "\\'") + "')", myConn);
                         command5.ExecuteNonQuery();
                         command5.Dispose();

                         command5 = new MySqlCommand("update " + TABLE_CC_TRANSACTIONS + " set cc_reference_id = '" + response["PNREF"].ToString() + "', cc_auth_code = '" + authCode + "', cc_transactions_message= '" + response["RESULT"].ToString()+ ":"+ response["RESPMSG"].ToString() + "' where cc_transactions_id = '" + cc_transactions_id.ToString() + "'", myConn);
                         command5.ExecuteNonQuery();
                         command5.Dispose();

                         // Update our renewal orders cc expiration date.
                         command5 = new MySqlCommand("update orders set cc_expires = '" + cc_expires + "' where orders_id = '" + renewal_orders_id.ToString() + "'", myConn);
                         command5.ExecuteNonQuery();
                         command5.Dispose();

                         // Update the associated payment card details expiration date as well.
                         command5 = new MySqlCommand("update payment_cards set cc_expires = '" + cc_expires + "' where payment_cards_id = '" + payment_cards_id.ToString() + "'", myConn);
                         command5.ExecuteNonQuery();
                         command5.Dispose();

                         //update the order so it won't get pulled again.
                         command5 = new MySqlCommand("update orders set renewal_transaction_date = null where orders_id = '" + orders_id.ToString() + "'", myConn);
                         command5.ExecuteNonQuery();
                         command5.Dispose();

                         number_of_renewal_charged++;
                    }
                }
                myReader.Close();
                return number_of_renewal_charged;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        //This function will set the billing series in motion.
        private static int create_first_effort_renewal_invoices()
        {
            try
            {
                //$renewals_billing_series_array = unserialize(RENEWALS_BILLING_SERIES);
                
                int number_of_renewal_invoices_created = 0;
                int renewals_billing_series_id;
                int renewals_billing_series_delay;
                int renewals_billing_series_effort_number;
                bool mysqlError = false;

                //loop through each billing series and create a renewal invoice for any orders that needs it.
                //add in the delay for each effort
                foreach (DataRow rs in renewals_billing_series_array.Rows)
                {
                    renewals_billing_series_id = Convert.ToInt32(rs["renewals_billing_series_id"]);
                    renewals_billing_series_delay = Convert.ToInt32(rs["delay_in_days"]);
                    renewals_billing_series_effort_number = Convert.ToInt32(rs["effort_number"]);

                    //this is only the first effort so move on to the next if this is effort 2 and above.
                    if (renewals_billing_series_effort_number > 1)
                    {
                        continue;
                    }
                    //only grab the last 30 days worth. No need to get all orders ever.
                    string renewal_orders_query_string = @"
			select
				o.*, op.*, s.*, p.continuous_service, p.products_status
			from
				orders o,
				orders_products op,
				products p,
				skus s
			where
				o.orders_id = op.orders_id
				and op.products_id = p.products_id
				and op.skus_id = s.skus_id
				and o.renewal_invoices_created = 0
				and o.renewal_invoices_sent = 0
				and o.orders_status = 1
				and o.is_renewal_order = 1
				and o.renewals_billing_series_id = " + renewals_billing_series_id.ToString() + @"
				and to_days(o.date_purchased) > to_days(DATE_SUB(curdate(),INTERVAL 60 DAY))
				and to_days(o.date_purchased) <= to_days(DATE_SUB(curdate(),INTERVAL " + renewals_billing_series_delay + @" DAY))
		";

                    //we will create an invoice for any valid renewal orders (including ones that have
                    //continuous service = 0 or auto_renew = 0, this will be caught in the email sending.
                    //that way it will not be pulled again for invoicing since the
                    //renwal_invoices_created will be 1.
                    //we will then check before sending it if the order is still valid.

                    command = new MySqlCommand(renewal_orders_query_string, myConn);
                    command.ExecuteNonQuery();

                    MySqlDataReader myReader;
                    myReader = command.ExecuteReader();

                    while (myReader.Read())
                    {
                        renewal_orders_id = Convert.ToInt32(myReader["orders_id"]);
                        renewal_orders_customers_id = Convert.ToInt32(myReader["customers_id"]);
                        products_status = Convert.ToInt32(myReader["products_status"]);
                        skus_status = Convert.ToInt32(myReader["skus_status"]);
                        continuous_service = Convert.ToInt32(myReader["continuous_service"]);
                        products_id = Convert.ToInt32(myReader["products_id"]);
                        skus_type_order = Convert.ToInt32(myReader["skus_type_order"]);
                        prior_orders_id = Convert.ToInt32(myReader["prior_orders_id"]);
                        auto_renew = Convert.ToInt32(myReader["auto_renew"]);

                        //Let's check to make sure the user hasn't already been entered for the same order
                        //if so the unique index will be violated and an error returned. Using the tep_db_query_return_error version of the
                        // it will allow us to continue. Which is what we want here.
                        string create_renewal_invoice_query_string = @"insert into renewals_invoices (date_to_be_sent, orders_id, customers_id, renewals_billing_series_id, effort_number, in_progress)
                            values (now(), '" + renewal_orders_id.ToString() + "', '" + renewal_orders_customers_id.ToString() + "', '" + renewals_billing_series_id.ToString() + "', '1', '1')";
                        
                        try
                        {
                            command5 = new MySqlCommand(create_renewal_invoice_query_string, myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                        catch (MySqlException ex)
                        {
                            Console.WriteLine(ex.Message);
                            if (ex.Message.Contains("Duplicate"))
                            {
                                mysqlError = true;
                                //if there was an error let's record that.
                                log_renewal_process("Warning: create_renewal_invoice tried to insert the same user,same order, same effort (" + create_renewal_invoice_query_string + ")", orders_id);
                            }
                        }
                        //if there was an error or not, we need to update the order so it won't get pulled again.
                        command5 = new MySqlCommand("update orders set renewal_invoices_created = 1 where orders_id = '" + renewal_orders_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                        number_of_renewal_invoices_created++;
                    }
                    myReader.Close();
                }
                return number_of_renewal_invoices_created;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static int create_additional_renewal_invoices()
        {
            //go through only pending orders, which haven"t been sent yet and are in progress
            try
            {
                int number_of_renewal_invoices_created = 0;
                int n = renewals_billing_series_array.Rows.Count;
                string comment = string.Empty;
                string next_effort_delay = string.Empty;
                bool existsNextEffort = false;
                bool create_next_effort;
                bool mysqlError = false;
                //rearrange the billing series array so we can pick the next effort
                //for($i=0, $n=sizeof($renewals_billing_series_array); $i<$n;$i++) {
		        //$renewels_billing_series[$renewals_billing_series_array[$i]['renewals_billing_series_id']][$renewals_billing_series_array[$i]['effort_number']] = $renewals_billing_series_array[$i];
	            //}

                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = @"
		            select *
		            from renewals_invoices ri,
			            orders o,
			            orders_products op,
			            renewals_billing_series rbs,
			            skus s,
			            products p
		            where ri.orders_id=o.orders_id
			            and o.orders_id = op.orders_id
			            and op.skus_id = s.skus_id
			            and op.products_id = p.products_id
			            and o.renewals_billing_series_id = rbs.renewals_billing_series_id
			            and rbs.renewals_billing_series_id = ri.renewals_billing_series_id
			            and rbs.effort_number = ri.effort_number
			            and ri.was_sent=1
			            and ri.in_progress=1
	            ";
                command.ExecuteNonQuery();

                MySqlDataReader myReader;
                myReader = command.ExecuteReader();
                while (myReader.Read())
                {
                    renewals_invoices_id = Convert.ToInt32(myReader["renewals_invoices_id"]);
                    customers_id = Convert.ToInt32(myReader["customers_id"]);
                    orders_id = Convert.ToInt32(myReader["orders_id"]);
                    renewals_billing_series_id = Convert.ToInt32(myReader["renewals_billing_series_id"]);
                    renewals_billing_series_effort_number = Convert.ToInt32(myReader["effort_number"]);
                    products_id = Convert.ToInt32(myReader["products_id"]);
                    skus_type_order = Convert.ToInt32(myReader["skus_type_order"]);
                    prior_orders_id = Convert.ToInt32(myReader["prior_orders_id"]);
                    renewal_order_status = Convert.ToInt32(myReader["orders_status"]);
                    skus_status = Convert.ToInt32(myReader["skus_status"]);
                    date_sent = Convert.ToDateTime(myReader["date_sent"]);
                    continuous_service = Convert.ToInt32(myReader["continuous_service"]);
                    auto_renew = Convert.ToInt32(myReader["auto_renew"]);

                    create_next_effort = true;
                    int next_effort_number = renewals_billing_series_effort_number + 1;

                    //check to see if the order is still valid for invoice creation, if not then update the invoice
                    //and move on to next order.
                    check_renewal_order_result = check_renewal_order();
                    if (check_renewal_order_result != "true")
                    {
                        comment = "Next effort for this invoice was not created because " + check_renewal_order_result;
                        comment = comment.Replace("'", "\\'");
                        command5 = new MySqlCommand("update renewals_invoices set in_progress = 0, comments='" + comment + "' where renewals_invoices_id = '" + renewals_invoices_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                        continue;
                    }
                    //check to see if there is a next effort for this series.
                    foreach (DataRow dr in renewals_billing_series_array.Rows)
                    {
                        if (dr["renewals_billing_series_id"].ToString() == renewals_billing_series_id.ToString() && dr["effort_number"].ToString() == next_effort_number.ToString())
                        {
                            existsNextEffort = true;
                            next_effort_delay = dr["delay_in_days"].ToString();
                            break;
                        }
                        else
                        {
                            existsNextEffort = false;
                        }
                    }

                    if (existsNextEffort == false)
                    {
                        create_next_effort = false;
                        comment = "Next effort for this invoice was not created because there are no more efforts for this billing series.";
                    }
                    else
                    {
                        comment = "Next effort for this invoice was created.";
                    }

                    if (create_next_effort)
                    {
                        //this is where we create the next one.
                        //Let's check to make sure the user hasn't already been entered for the same order
                        //if so the unique index will be violated and an error returned. Using the tep_db_query_return_error version of the
                        // it will allow us to continue. Which is what we want here. We add the delay here.
                        string create_renewal_invoice_query_string2 = @"insert into renewals_invoices (date_to_be_sent, orders_id, customers_id, renewals_billing_series_id, effort_number, in_progress)
                        values (DATE_ADD(curdate(),INTERVAL " + next_effort_delay + " DAY), '" + orders_id.ToString() + "', '" + customers_id.ToString() + "', '" + renewals_billing_series_id.ToString() + "', " + next_effort_number.ToString() + ", '1')";

                        try
                        {
                            command5 = new MySqlCommand(create_renewal_invoice_query_string2, myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                        catch (MySqlException ex)
                        {
                            Console.WriteLine(ex.Message);
                            if (ex.Message.Contains("Duplicate"))
                            {
                                mysqlError = true;
                                //if there was an error let's record that.
                                log_renewal_process("Warning: create_additional_renewal_invoice tried to insert the same user,same order, same effort (" + create_renewal_invoice_query_string2 + ")", orders_id);
                            }
                        }
                        number_of_renewal_invoices_created++;
                    }

                    //set this invoice' in_progress to 0. Used for clean up later.
                    if (create_next_effort)
                    {
                        comment = comment.Replace("'", "\\'");
                        command5 = new MySqlCommand("update renewals_invoices set in_progress = 0, comments='" + comment + "' where renewals_invoices_id = '" + renewals_invoices_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                    }
                    else
                    {
                        //don't set the in_progress to 0 since it is the last effort. We'll clean this one up during
			            //mass cancel,since it is allowed to be active for cancel delay days.
                        comment = comment.Replace("'", "\\'");
                        command5 = new MySqlCommand("update renewals_invoices set comments='" + comment + "' where renewals_invoices_id = '" + renewals_invoices_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                    }
                }
                myReader.Close();
                return number_of_renewal_invoices_created;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static int send_renewal_email_invoices()
        {
            try
            {
	            currency = (USE_DEFAULT_LANGUAGE_CURRENCY == "true") ? LANGUAGE_CURRENCY : DEFAULT_CURRENCY;
	            //$currencies = new currencies(); 
                bool is_gift2;
                //go through only pending orders, which haven"t been sent yet and are in progress
                int number_of_email_renewal_invoices_sent = 0;

                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = @"select *
				from renewals_invoices ri,
													 orders o,
													 orders_products op,
												     renewals_billing_series rbs,
 													 skus s,
                                                     products p,
        											 products_description pd
												where ri.orders_id=o.orders_id
												and o.orders_id = op.orders_id
												and op.skus_id = s.skus_id
												and op.products_id = p.products_id
												and p.products_id = pd.products_id
												and pd.language_id = 1
												and o.renewals_billing_series_id = rbs.renewals_billing_series_id
 												and rbs.renewals_billing_series_id = ri.renewals_billing_series_id
        										and rbs.effort_number = ri.effort_number
												and ri.was_sent=0
                  								and ri.in_progress=1
												and to_days(ri.date_to_be_sent) <= to_days(curdate())
 												and rbs.renewals_invoices_type = 'EMAIL'";
                command.ExecuteNonQuery();
                string renewals_email_name = string.Empty;
                string renewals_invoices_email_name = string.Empty;
                MySqlDataReader myReader;
                myReader = command.ExecuteReader();

                while (myReader.Read())
                {
                    renewals_invoices_id = Convert.ToInt32(myReader["renewals_invoices_id"]);
                    renewals_invoices_email_name = myReader["renewals_invoices_email_name"].ToString();
                    customers_id = Convert.ToInt32(myReader["customers_id"]);
                    orders_id = Convert.ToInt32(myReader["orders_id"]);
                    renewals_billing_series_id = Convert.ToInt32(myReader["renewals_billing_series_id"]);
                    accepted_for_delivery = false;
                    products_id = Convert.ToInt32(myReader["products_id"]);
                    skus_id = Convert.ToInt32(myReader["skus_id"]);
                    skus_type_order = Convert.ToInt32(myReader["skus_type_order"]);
                    prior_orders_id = Convert.ToInt32(myReader["prior_orders_id"]);
                    renewal_order_status = Convert.ToInt32(myReader["orders_status"]);
                    skus_status = Convert.ToInt32(myReader["skus_status"]);
                    continuous_service = Convert.ToInt32(myReader["continuous_service"]);
                    auto_renew = Convert.ToInt32(myReader["auto_renew"]);
                    is_gift = Convert.ToInt32(myReader["is_gift"].ToString());
                    skinsites_id = Convert.ToInt32(myReader["skinsites_id"]);
                    is_postcard_confirmation = Convert.ToInt32(myReader["is_postcard_confirmation"]);

                    // check to see if order is a postcard confirmation. If it is, mark the invoice as sent and move to next order (no e-mail).
                    if (is_postcard_confirmation == 1)
                    {
                        number_of_email_renewal_invoices_sent++;
                        //update the was_sent flag.
                        command5 = new MySqlCommand(@"update renewals_invoices
						  set was_sent=1, date_sent=now()
						  where renewals_invoices_id='" +renewals_invoices_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();

                        //update the order's invoices_sent flag. so it will be pulled for charging.
                        command5 = new MySqlCommand("update orders set renewal_invoices_sent=1 where orders_id='" + orders_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                        continue;
                    }

                    if (is_gift == 1)
                    {
                        is_gift2 = true;
                    }
                    else
                    {
                        is_gift2 = false;
                    }
                    //check to see if the order is still valid for invoice sending
                    check_renewal_order_result = check_renewal_order();
                    if (check_renewal_order_result != "true") {
                        comments = "This email effort was not sent because " + check_renewal_order_result;
                        command5 = new MySqlCommand("update renewals_invoices set in_progress = 0, comments = '" + comments + "' where renewals_invoices_id = '" + renewals_invoices_id.ToString() + "'", myConn);
                        command5.ExecuteNonQuery();
                        command5.Dispose();
                        continue;
                    }
                    if (renewals_invoices_email_name == string.Empty)
                    {
                        log_renewal_process("ERROR: Renewal email name is not set for billing series id: " + renewals_billing_series_id.ToString());
                    }
                    else
                    {
                        //first the include doesn't have a return stmt so it will set $found_include_file to 1 if
                        //the inlude worked. The included file will set $accepted_for_delivery if the email was sent.
                        string tplDir = get_template_dir(skinsites_id);
                        bool found_include_file = File.Exists(tplDir + "/" + renewals_invoices_email_name);
                        if (accepted_for_delivery)
                        {
                            number_of_email_renewal_invoices_sent++;
                            //update the was_sent flag.
                            command5 = new MySqlCommand(@"update renewals_invoices
    						  set was_sent=1, date_sent=now()
    						  where renewals_invoices_id='" + renewals_invoices_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                            //update the order's invoices_sent flag. so it will be pulled for charging.
                            command5 = new MySqlCommand(@"update orders set renewal_invoices_sent=1 where orders_id='" + orders_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                        else
                        {
                            //if the include wasn't found see why. If the mail wasn't sent we will try again tomorrow, since the
                            //was_sent flag wasn't updated.
                            if (!found_include_file)
                            {
                                log_renewal_process("ERROR: Unable to find renewal email: " + renewals_invoices_email_name);
                            }
                        }

                    }
                }
                myReader.Close();
                return number_of_email_renewal_invoices_sent;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static int create_renewal_paper_invoices_file()
        {
            try
            {
                // Go through only pending orders, which haven"t been sent yet and are in progress
                string renewal_invoices_info_query_string = @"select *
												from renewals_invoices ri,
													 orders o,
													 orders_products op,
												     renewals_billing_series rbs,
 													 skus s,
 													 products p,
												     skinsites ss
												where ss.skinsites_id = o.skinsites_id
											  	and ri.orders_id=o.orders_id
												and o.orders_id = op.orders_id
												and op.skus_id = s.skus_id
												and op.products_id = p.products_id
												and o.renewals_billing_series_id = rbs.renewals_billing_series_id
 												and rbs.renewals_billing_series_id = ri.renewals_billing_series_id
        										and rbs.effort_number = ri.effort_number
												and ri.was_sent=0
                  								and ri.in_progress=1
												and to_days(ri.date_to_be_sent) <= to_days(curdate())
 												and rbs.renewals_invoices_type = 'PAPER'";

                // Set our number of processed paper invoices to its default value of zero.
                int number_of_renewal_paper_invoices_file_records = 0;
                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = renewal_invoices_info_query_string;
                command.ExecuteNonQuery();
                MySqlDataReader myReader;
                myReader = command.ExecuteReader();
                while (myReader.Read())
                {
                    // Pull data form our current renewal invoice.
                    billing_first_name = myReader["billing_first_name"].ToString();
                    billing_last_name = myReader["billing_last_name"].ToString();
                    billing_address_line_1 = myReader["billing_street_address"].ToString();
                    billing_city = myReader["billing_city"].ToString();
                    billing_state = myReader["billing_state"].ToString();
                    billing_postal_code = myReader["billing_postcode"].ToString();
                    delivery_first_name = myReader["delivery_first_name"].ToString();
                    delivery_last_name = myReader["delivery_last_name"].ToString();
                    delivery_address_line_1 = myReader["delivery_street_address"].ToString();
                    delivery_city = myReader["delivery_city"].ToString();
                    delivery_state = myReader["delivery_state"].ToString();
                    delivery_postal_code = myReader["delivery_postcode"].ToString();
                    renewals_invoices_id = Convert.ToInt32(myReader["renewals_invoices_id"]);
                    customers_id = Convert.ToInt32(myReader["customers_id"]);
                    orders_id = Convert.ToInt32(myReader["orders_id"]);
                    renewals_billing_series_code = myReader["renewals_billing_series_code"].ToString();
                    products_id = Convert.ToInt32(myReader["products_id"]);
                    skus_type_order = Convert.ToInt32(myReader["skus_type_order"]);
                    prior_orders_id = Convert.ToInt32(myReader["prior_orders_id"]);
                    renewal_order_status = Convert.ToInt32(myReader["orders_status"]);
                    skus_status = Convert.ToInt32(myReader["skus_status"]);
                    products_name = myReader["products_name"].ToString();
                    skus_term = Convert.ToInt32(myReader["skus_term"]);
                    effort_number = Convert.ToInt32(myReader["effort_number"]);
                    date_purchased = Convert.ToDateTime(myReader["date_purchased"]);
                    amount_owed = Convert.ToDouble(myReader["amount_owed"]);
                    amount_paid = Convert.ToDouble(myReader["amount_paid"]);
                    price = Convert.ToDouble(myReader["products_price"]);
                    email_address = myReader["customers_email_address"].ToString();
                    continuous_service = Convert.ToInt32(myReader["continuous_service"]);
                    auto_renew = Convert.ToInt32(myReader["auto_renew"]);
                    cc_number_display = myReader["cc_number_display"].ToString();
                    template_directory = myReader["tplDir"].ToString();
                    skinsites_id = Convert.ToInt32(myReader["skinsites_id"]);
                }
                myReader.Close();
                return number_of_renewal_paper_invoices_file_records;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static int clean_up_renewal_invoices()
        {
            //move any renewal invoice where in progress is 0 or was sent.
            try
            {
                int number_of_renewal_invoices_cleaned_up = 0;
                int renewals_invoices_id;

                //LOOP THROUHG ALL INVOICES WHERE IN_PROGRESS IS 0.
                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = "select * from renewals_invoices ri where ri.in_progress=0";
                command.ExecuteNonQuery();

                MySqlDataReader myReader;
                myReader = command.ExecuteReader();

                while (myReader.Read())
                {
                    Console.WriteLine(myReader["renewals_invoices_id"]);
                    renewals_invoices_id = Convert.ToInt32(myReader["renewals_invoices_id"]);

                    //move the invoice to history.
                    //we use replace. If the server goes down right between these 2 stmts then the next time
                    //it will still work.
                    command2 =
                        new MySqlCommand(
                            "replace into renewals_invoices_history select * from renewals_invoices where renewals_invoices_id = '" +
                            renewals_invoices_id.ToString() + "'", myConn);
                    command2.ExecuteNonQuery();
                    //remove old one
                    command3 =
                        new MySqlCommand(
                            "delete from renewals_invoices where renewals_invoices_id = '" +
                            renewals_invoices_id.ToString() + "'", myConn);
                    command3.ExecuteNonQuery();

                    number_of_renewal_invoices_cleaned_up++;
                }
                myReader.Close();
                return number_of_renewal_invoices_cleaned_up;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static int mass_cancel_renewal_orders()
        {
            //mass cancel renewal orders. We look at the cancel_delay on the billing series
            //which we will add to the time the last invoice was sent and if the time
            //has expired we simply cancel the order (if it was still Pending).
            //we also need to cancel any still Pending orders that have moved to history.
            try
            {
                int number_of_mass_cancelled_orders = 0;

                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = @"select ri.*, o.*, rbs.*
												from renewals_invoices ri,
													 orders o,
												     renewals_billing_series rbs
												where ri.orders_id=o.orders_id
												and o.renewals_billing_series_id = rbs.renewals_billing_series_id
 												and rbs.renewals_billing_series_id = ri.renewals_billing_series_id
        										and rbs.effort_number = ri.effort_number
												and o.orders_status = 1
												and ri.in_progress = 1
												and rbs.cancel_delay is not null
												and to_days(now()) > to_days(DATE_ADD(ri.date_sent,INTERVAL rbs.cancel_delay DAY))";
                command.ExecuteNonQuery();
                return number_of_mass_cancelled_orders;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static string get_renewal_date(int orders_id)
        {
            try
            {
                string renewal_lead_time;
                DateTime renewal_date;
                int skus_days_spanned = 0;

                if (orders_id.ToString() == string.Empty)
                {
                    return string.Empty;
                }

                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = @"select
			            s.renewal_lead_time as skus_renewal_lead_time,
			            s.skus_days_spanned,
			            p.renewal_lead_time as products_renewal_lead_time,
			            date_format(fbi.date_added, '%Y-%m-%d') as date_paid,
			            s.skus_type,
			            o.is_postcard_confirmation
		            from " +
                                      TABLE_ORDERS + @" o, " +
                                      TABLE_FULFILLMENT_BATCH_ITEMS + @" fbi, " +
                                      TABLE_FULFILLMENT_BATCH + @" fb, " +
                                      TABLE_ORDERS_PRODUCTS + @" op, " +
                                      TABLE_SKUS + @" s, " +
                                      TABLE_PRODUCTS + @" p
		            where
			            o.orders_id = op.orders_id
			            and o.orders_id = fbi.orders_id
			            and fbi.fulfillment_batch_id = fb.fulfillment_batch_id
			            and fb.fulfillment_status_id = 1
			            and op.products_id = p.products_id
			            and op.skus_id = s.skus_id
			            and o.orders_id = '" + orders_id.ToString() + @"'
		            order by fbi.date_added desc
		            limit 1";

                command.ExecuteNonQuery();
                MySqlDataReader read;
                read = command.ExecuteReader();
                while (read.Read())
                {
                    skus_days_spanned = Convert.ToInt32(read["skus_days_spanned"]);
                }
                //first check sku, then product finally use default renewal lead time.
                if (Convert.ToInt32(read["skus_renewal_lead_time"]) != 0)
                {
                    renewal_lead_time = read["products_renewal_lead_time"].ToString();
                }
                else
                {
                    renewal_lead_time = DEFAULT_RENEWAL_LEADTIME;
                }
                if (read["skus_type"].ToString() == "INTRO")
                {
                    //renewal notice date for new orders is date paid + days_spanned - leadtime
                    renewal_date = DateTime.Parse(read["date_paid"].ToString());
                    renewal_date = renewal_date.AddDays(skus_days_spanned);
                    renewal_date = renewal_date.AddDays(Convert.ToInt32(renewal_lead_time) * -1);
                }
                else
                {
                    //renewal date for renewal orders is order_date + days_spanned
                    //no need to add lead time it is already built in since this is a renewal.
                    //so instread of  order_date + days_spanned - leadtime (which is for INTRO skus)
                    //we only need order_date + days_spanned
                    renewal_date = DateTime.Parse(read["date_paid"].ToString());
                    renewal_date = renewal_date.AddDays(skus_days_spanned);
                }

                // 	If is_postcard_confirmation, modify the final renewal_date by ADDING the value from the configuration key
                // 	DEFAULT_RENEWAL_CHARGE_DAYS and SUBTRACTING the value from the configuration key
                // 	RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS.
                if (read["is_postcard_confirmation"].ToString() == "1")
                {
                    renewal_date = renewal_date.AddDays(Convert.ToInt32(DEFAULT_RENEWAL_CHARGE_DAYS));
                    renewal_date = renewal_date.AddDays(Convert.ToInt32(RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS)*-1);
                }
                command.Dispose();
                read.Close();
                return renewal_date.ToString("yyyy-MM-dd HH:mm:ss");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return string.Empty;
            }
        }

        private static string check_renewal_order()
        {
            //If a renewal order is placed, at the time of the sending of email or charging the card,
            //or getting check, the product and sku could be changed to
            //inactive. If the product is inactive and there are no renewal sku active at all for that
            //product then don"t send email and don"t renew.
            // if the product is inactive and the skus_id (on orders_products.skus_id on the renewal order
            // pulled here) is inactive (Matt has changed the price/remit)
            // then we need to do a quick check to see if there is at least another active for that
            // product for the same skus_type_order. Fulfillment will take care of the rest.
            try
            {
                check_renewal_order_result = "true";
                if (skus_status == 0)
                {
                    string skus_status_check_query = "select * from skus where products_id = '" + products_id.ToString() +
                                                     "' and skus_type = 'RENEW' and skus_type_order = '" +
                                                     skus_type_order.ToString() + "' and skus_status = '1'";
                    command = new MySqlCommand(skus_status_check_query, myConn);
                    command.ExecuteNonQuery();
                    MySqlDataReader myReader;
                    myReader = command.ExecuteReader();

                    if (!myReader.HasRows)
                    {
                        //no active renewal skus so don't create an invoice will be removed later.

                        //$update_sql2 = "update " . TABLE_ORDERS . " set renewal_error='1', renewal_error_description='Error: existing renewal sku is inactive for this order.' where orders_id=$prior_orders_id";
                        //tep_db_query($update_sql2);

                        check_renewal_order_result = "No active renewal sku (type order " + skus_type_order.ToString() +
                                                     ") for products_id " + products_id.ToString();
                    }
                }
                //make sure this renewal order product's continuous service is still active and this renewal orders
                //auto renew hasn't been changed.
                if (continuous_service != 1)
                {
                    check_renewal_order_result = "Continuous Service for products_id " + products_id.ToString() +
                                                 " is not 1.";
                }
                if (renewal_order_auto_renew != 1)
                {
                    check_renewal_order_result = "Auto Renew for this order is not 1.";
                }
                if (renewal_order_status != 1)
                {
                    check_renewal_order_result = "This orders status is " + renewal_order_status.ToString() +
                                                 ". Only Pending orders are valid.";
                }
                // Also make sure we check that the original order wasn't cancelled yet or auto_renew has been reset to 0.
                if (prior_orders_id.ToString() != "")
                {
                    command =
                        new MySqlCommand("select * from orders where orders_id = '" + prior_orders_id.ToString() + "'",
                            myConn);
                    command.ExecuteNonQuery();
                    MySqlDataReader myReader;
                    myReader = command.ExecuteReader();

                    while (myReader.Read())
                    {
                        prior_order_status = Convert.ToInt32(myReader["orders_status"]);
                        prior_order_auto_renew = Convert.ToInt32(myReader["auto_renew"]);
                    }

                    //if the original order isn't a Paid order (cancelled or disputed) go on to the next order.
                    //if the original order's auto-renew (user action) was changed move on the next order.
                    if (prior_order_status != 2)
                    {
                        check_renewal_order_result = "The original order (orders_id: " + prior_orders_id.ToString() +
                                                     ") is no longer Paid.";
                    }
                    if (prior_order_auto_renew == 0)
                    {
                        check_renewal_order_result = "The auto renew for the original order (orders_id: " +
                                                     prior_orders_id.ToString() + ") is not 1.";
                    }
                }
                return check_renewal_order_result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return string.Empty;
            }
        }

        private static int create_renewal_order(Dictionary<string, object> order, int renewals_billing_series_id, bool is_perfect_renewal,
            Dictionary<string, object> renewal_sku, int is_postcard_confirmation)
        {
            try
            {
                string queryString = string.Empty;
                int renewal_orders_id = 0;

                int n = orders_columns.Count;
                int j = orders_products_columns.Count;
                string column_name = string.Empty;

                renewal_order = new Dictionary<string, object>();
                renewal_order_product = new Dictionary<string, object>();
                renewal_order_status_history = new Dictionary<string, object>();
                renewal_order_total = new Dictionary<string, object>();
                renewal_order_subtotal = new Dictionary<string, object>();

                //now loop through each and create an array for each table with data from $order.
                //this allows us to just override the columns we want and have the rest automatically
                //copied over.

                for (int i = 0; i < n; i++)
                {
                    column_name = orders_columns[i].ToString();
                    renewal_order.Add(column_name, order[column_name]);
                }

                for (int x = 0; x < j; x++)
                {
                    column_name = orders_products_columns[x].ToString();
                    renewal_order_product.Add(column_name, order[column_name]);
                }
                //creates the parent order number.
                //and set it on the new order.
                command = new MySqlCommand("insert into orders_groups (orders_groups_id) VALUES ('')", myConn);
                command.ExecuteNonQuery();
                int renewal_orders_groups_id = Convert.ToInt32(command.LastInsertedId);
                renewal_order["orders_groups_id"] = renewal_orders_groups_id;
                //If renewal cc data exists (renewal_payment_cards_id) on the original order, use it to set the cc fields for the renewal
                if (renewal_order["renewal_payment_cards_id"].ToString() != string.Empty)
                {
                    renewal_order["cc_type"] = order["renewal_cc_type"];
                    renewal_order["cc_owner"] = order["renewal_cc_owner"];
                    renewal_order["cc_expires"] = order["renewal_cc_expires"];
                    renewal_order["cc_number"] = order["renewal_cc_number"];
                    renewal_order["cc_number_display"] = order["renewal_cc_number_display"];
                    renewal_order["payment_cards_id"] = order["renewal_payment_cards_id"];
                    renewal_order["payment_method"] = "cc";
                }

                // Account for "partner_paid" orders by setting them to "cc" for the renewal order (MCS 3/2015)
                if (renewal_order["payment_method"].ToString() == "partner_paid")
                {
                    renewal_order["payment_method"] = "cc";
                }
                //get rid of the unwanted fields
                renewal_order.Remove("orders_id");
                renewal_order.Remove("renewal_payment_cards_id");
                //THIS IS COMMENTED OUT SINCE IT SHOULD STAY A CC ORDER. IF IT IS A TRACK 2 IT WON'T GET PULLED
                //FOR CHARGING AND IF CHECK COMES IN, CUSTCARE CAN CHANGE IT THERE TO A CHECK ENTRY.
                //overwrite fields.
                //if this isn't a perfect renewal make this an invoice order.
                //but leave the credit card info in place for next year renewals (especially Master Card)
                //if (!$is_perfect_renewal) {
                //    	$renewal_order['payment_method'] = 'check';
                //     $renewal_order['cc_type'] = '';
                //     $renewal_order['cc_owner'] = '';
                //     $renewal_order['cc_number'] = '';
                //     $renewal_order['cc_expires']   = '';
                //     $renewal_order['cc_number_display']   = '';
                //     $renewal_order['payment_cards_id'] = 0;
                //}
                renewal_order["last_modified"] = "null";
                renewal_order["date_purchased"] = "now()";
                //set to pending.
                renewal_order["orders_status"] = DEFAULT_ORDERS_STATUS_ID;
                renewal_order["orders_date_finished"] = "null";
                renewal_order["source_id"] = "null";
                renewal_order["source_id_time_entered"] = "null";
                renewal_order["source_id_type"] = "null";
                renewal_order["mystery_gifts_id"] = "null";
                renewal_order["quickshop_used"] = 0;
                //get the price from the renewal sku.
                renewal_order["amount_owed"] = renewal_sku["skus_price"];
                renewal_order["amount_paid"] = 0;
                renewal_order["is_buyagain"] = "0";
                renewal_order["fulfillment_batch_id"] = "null";
                renewal_order["skus_id_used_for_fulfillment"] = 0;
                //renewal_date will be filled in when the the order is paid (when adding fulfill batch_item).
                renewal_order["renewal_date"] = "null";
                renewal_order["renewal_invoices_created"] = 0;
                renewal_order["renewal_invoices_sent"] = 0;
                //end_delivery_range will be setup when the order is paid(when adding fulfill batch_item).
                renewal_order["end_delivery_range"] = "";
                renewal_order["renewal_transaction_date"] = "null";
                renewal_order["renewal_orders_id"] = "null";
                renewal_order["prior_orders_id"] = order["orders_id"];
                renewal_order["is_renewal_order"] = 1;
                renewal_order["renewals_billing_series_id"] = renewals_billing_series_id;
                renewal_order["is_gift"] = order["is_gift"];
                renewal_order["renewals_credit_card_charge_attempts"] = 0;

                if (is_postcard_confirmation == 1)
                {
                    renewal_order["is_postcard_confirmation"] = "1";
                    //if (is_postcard_confirmation == 1)
                    renewal_order["postcard_confirmation_date"] = "now()";
                }

                //clear our delayed billing data.
                renewal_order["is_delayed_billing"] = 0;
                renewal_order["is_delayed_billing_paid"] = 0;
                renewal_order["delayed_billing_date"] = "null";
                renewal_order["delayed_billing_credit_card_charge_attempts"] = 0;

                // clear renewal error
                renewal_order["renewal_error"] = 0;
                renewal_order["renewal_error_description"] = "";

                //this used to be on the original order now moved here.
                if (is_perfect_renewal)
                {
                    if (is_postcard_confirmation == 1)
                    {
                        renewal_order["renewal_transaction_date"] = "date_add(now(), INTERVAL " +
                                                                    RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS + " DAY)";
                    }
                    else
                    {
                        renewal_order["renewal_transaction_date"] = "date_add(now(), INTERVAL " +
                                                                    DEFAULT_RENEWAL_CHARGE_DAYS + " DAY)";
                    }
                }

                queryString = tep_db_perform("orders", renewal_order);
                command2 = new MySqlCommand(queryString, myConn);
                command.ExecuteNonQuery();
                renewal_orders_id = Convert.ToInt32(command.LastInsertedId);

                if (renewal_orders_id != 0 || renewal_orders_id.ToString() != string.Empty)
                {
                    //orders_product table overwrites.
                    //unset($renewal_order_product['orders_products_id']);
                    renewal_order_product["orders_id"] = renewal_orders_id;
                    renewal_order_product["skus_id"] = renewal_sku["skus_id"];
                    renewal_order_product["products_price"] = renewal_sku["skus_price"];
                    renewal_order_product["final_price"] = renewal_sku["skus_price"];
                    renewal_order_product["location"] = "renewal";

                    queryString = tep_db_perform("orders_products", renewal_order_product);
                    command3 = new MySqlCommand(queryString, myConn);
                    command3.ExecuteNonQuery();

                    //order_status_history
                    renewal_order_status_history["orders_id"] = renewal_orders_id;
                    renewal_order_status_history["orders_status_id"] = DEFAULT_ORDERS_STATUS_ID;
                    renewal_order_status_history["date_added"] = "now()";
                    renewal_order_status_history["comments"] = DEFAULT_PENDING_COMMENT;

                    queryString = tep_db_perform("orders_status_history", renewal_order_status_history);
                    command3 = new MySqlCommand(queryString, myConn);
                    command3.ExecuteNonQuery();

                    //order_total (mimicking what the ot_ classes do.
                    renewal_order_total["orders_id"] = renewal_orders_id;
                    renewal_order_total["title"] = "Total:";
                    renewal_order_total["text"] = "<b>" +
                                                  get_currency_format(renewal_sku["skus_price"].ToString(),
                                                      renewal_order["currency"].ToString()) + "</b>";
                    renewal_order_total["value"] = renewal_sku["skus_price"];
                    renewal_order_total["class"] = "ot_total";
                    renewal_order_total["sort_order"] = "800";
                    queryString =
                        tep_db_perform("orders_total", renewal_order_total);
                    command4 = new MySqlCommand(queryString, myConn);
                    command4.ExecuteNonQuery();
                    renewal_order_subtotal["orders_id"] = renewal_orders_id;
                    renewal_order_subtotal["title"] = "Sub-Total:";
                    renewal_order_subtotal["text"] = get_currency_format(renewal_sku["skus_price"].ToString(), renewal_order["currency"].ToString());
                    renewal_order_subtotal["value"] = renewal_sku["skus_price"];
                    renewal_order_subtotal["class"] = "ot_subtotal";
                    renewal_order_subtotal["sort_order"] = "1";
                    queryString = tep_db_perform("orders_total", renewal_order_subtotal);
                    command4 = new MySqlCommand(queryString, myConn);
                    command4.ExecuteNonQuery();
                }
                else
                {
                    log_renewal_process("ERROR: Unable to create renewal order.", Convert.ToInt32(order["orders_id"]));
                }
                debug(renewal_order, "renewal_order");
                debug(renewal_order_product, "renewal_order_product");
                debug(renewal_order_total, "renewal_order_total");
                debug(renewal_order_subtotal, "renewal_order_subtotal");
                debug(order, "order");
                return renewal_orders_id;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static void set_all_defines()
        {
            try
            {
                //countries table, for each country name give us the country code.
                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = "select * from countries";
                command.ExecuteNonQuery();
                MySqlDataReader myReader;
                myReader = command.ExecuteReader();
                countries = new Dictionary<string, object>();
                zones = new Dictionary<string, object>();
                configuration = new Dictionary<object, object>();

                while (myReader.Read())
                {
                    countries.Add(myReader["countries_name"].ToString(), myReader["countries_iso_code_3"]);
                }
                myReader.Close();
                //state names are used in order, for paper invoices we need to look up abbreviation for these state names
                command2 = new MySqlCommand(string.Empty, myConn);
                command2.CommandText = "select * from zones";
                command2.ExecuteNonQuery();

                MySqlDataReader myReader2;
                myReader2 = command2.ExecuteReader();

                while (myReader2.Read())
                {
                    zones.Add(myReader["zone_name"].ToString(), myReader["zone_code"]);
                }
                myReader2.Close();
                //currencies
                command3 = new MySqlCommand(string.Empty, myConn);
                command3.CommandText = "select code, title, symbol_left, symbol_right, decimal_point, thousands_point, decimal_places, value from currencies";
                command3.ExecuteNonQuery();

                currencies = new DataTable();

                using (MySqlDataAdapter da = new MySqlDataAdapter(command3))
                {
                    da.Fill(currencies);
                }
                //configuration table
                command4 = new MySqlCommand(string.Empty, myConn);
                command4.CommandText = "select * from configuration";
                command4.ExecuteNonQuery();

                MySqlDataReader myReader4;
                myReader4 = command4.ExecuteReader();

                while (myReader4.Read())
                {
                    configuration.Add(myReader4["configuration_key"], myReader4["configuration_value"]);
                }

                myReader4.Close();

                //get the right columns for all required tables, then set the data in the $renewal_order array

                command5 = new MySqlCommand(string.Empty, myConn);
                command5.CommandText = "show columns from orders";
                command5.ExecuteNonQuery();
                orders_columns = new List<string>();

                MySqlDataReader myReader5;
                myReader5 = command5.ExecuteReader();

                while (myReader5.Read())
                {
                    orders_columns.Add(myReader5["field"].ToString());
                }

                myReader5.Close();
                command6 = new MySqlCommand(string.Empty, myConn);
                command6.CommandText = "show columns from orders_products";
                command6.ExecuteNonQuery();
                orders_products_columns = new List<string>();

                MySqlDataReader myReader6;
                myReader6 = command6.ExecuteReader();

                while (myReader6.Read())
                {
                    orders_products_columns.Add(myReader6["field"].ToString());
                }

                myReader6.Close();

                //renewal billing series info.
                command7 = new MySqlCommand(string.Empty, myConn);
                command7.CommandText = @"select
			        renewals_billing_series_id,
			        effort_number,
			        delay_in_days,
			        renewals_billing_series_name,
			        renewals_invoices_type,
			        renewals_invoices_email_name
		        from
			        renewals_billing_series";

                command7.ExecuteNonQuery();
                renewals_billing_series_array = new DataTable();
                using (MySqlDataAdapter da = new MySqlDataAdapter(command7))
                {
                    da.Fill(renewals_billing_series_array);
                }
                command8 = new MySqlCommand(string.Empty, myConn);
                command8.CommandText = @"select * from skinsites";
                command8.ExecuteNonQuery();

                skinsites = new DataTable();

                using (MySqlDataAdapter da = new MySqlDataAdapter(command8))
                {
                    da.Fill(skinsites);
                }
                //setup the skinsite configurations
                command9 = new MySqlCommand(string.Empty, myConn);
                command9.CommandText =
                    @"select cs.skinsites_id, c.configuration_key as cfgKey, c.configuration_value as cfgValue, cs.skinsites_configuration_value from configuration c, configuration_skinsites cs where cs.configuration_id = c.configuration_id";
                command9.ExecuteNonQuery();

                skinsites_configuration_defines = new DataTable();

                using (MySqlDataAdapter da = new MySqlDataAdapter(command9))
                {
                    da.Fill(skinsites_configuration_defines);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static void log_renewal_process(string action, int orders_id)
        {
            try
            {
                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText =
                    "insert into renewal_process_log(date_entered, action, orders_id) values (now(), " + action + ", " +
                    orders_id.ToString() + ")";
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static void log_renewal_process(string action)
        {
            try
            {
                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText =
                    "insert into renewal_process_log(date_entered, action, orders_id) values (now(), " + action + ", " +
                    string.Empty + ")";
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static void create_fulfillment_batch_item(int orders_id, string fulfillment_status_id,
            string orders_previous_status = "")
        {
            try
            {
                if (orders_id.ToString() == "" || fulfillment_status_id.ToString() == "")
                {
                    return;
                }
                string paid_status=string.Empty;
                update_orders_fulfillment_status_id = true;
                //should always just have 1 orders_producst row so limit by 1.
                command =
                    new MySqlCommand(
                        @"select op.products_id, o.date_purchased, o.orders_status, o.fulfillment_batch_id, s.skus_type, s.skus_type_order, s.skus_type_order_period, if(p.first_issue_delay_days=0,pf.first_issue_delay_days, p.first_issue_delay_days) as first_issue_delay_days, o.prior_orders_id, o.is_renewal_order, s.skus_days_spanned from " +
                        TABLE_ORDERS_PRODUCTS + " op, " + TABLE_ORDERS + " o, " + TABLE_SKUS + " s, " + TABLE_PRODUCTS +
                        " p, " + TABLE_PUBLICATION_FREQUENCY +
                        " pf where op.products_id = p.products_id and p.publication_frequency_id = pf.publication_frequency_id and o.orders_id = op.orders_id and s.skus_id = op.skus_id and o.orders_id = '" +
                        orders_id.ToString() + "' limit 1", myConn);
                command.ExecuteNonQuery();

                MySqlDataReader order_product_info_array;
                order_product_info_array = command.ExecuteReader();

                while (order_product_info_array.Read())
                {
                    products_id = Convert.ToInt32(order_product_info_array["products_id"]);
                    first_issue_delay_days = Convert.ToInt32(order_product_info_array["first_issue_delay_days"]);
                    skus_type = order_product_info_array["skus_type"].ToString();
                    skus_type_order = Convert.ToInt32(order_product_info_array["skus_type_order"]);
                    skus_type_order_period = Convert.ToInt32(order_product_info_array["skus_type_order_period"]);
                    prior_orders_id = Convert.ToInt32(order_product_info_array["prior_orders_id"]);
                    is_renewal_order = Convert.ToInt32(order_product_info_array["is_renewal_order"]);
                    orders_status = Convert.ToInt32(order_product_info_array["orders_status"]);
                    days_spanned = Convert.ToInt32(order_product_info_array["skus_days_spanned"]);
                    order_fulfillment_batch_id = Convert.ToInt32(order_product_info_array["fulfillment_batch_id"]);
                    date_purchased = Convert.ToDateTime(order_product_info_array["date_purchased"]);
                }
              
                order_product_info_array.Close();
                //If the subscription is less than 6 months, only delay fulfillment to the next
                //batch week.

                // If there is a previous order id, use the previous orders skus_days_spanned
                if (prior_orders_id.ToString() != string.Empty)
                {
                    command5 = new MySqlCommand("select s.skus_days_spanned from " + TABLE_SKUS + " s,  " + TABLE_ORDERS_PRODUCTS + " op where s.products_id = op.products_id AND op.orders_id = '" + prior_orders_id.ToString() + "'", myConn);
                    command5.ExecuteNonQuery();
                    command5.Dispose();
                    MySqlDataReader prior_order;
                    prior_order = command5.ExecuteReader();

                    while (prior_order.Read())
                    {
                        days_spanned = Convert.ToInt32(prior_order["skus_days_spanned"]);
                    }
                }

                    if (days_spanned < 183)
                    {
                        renewal_fulfillment_delay = "DATE_ADD('" + date_purchased.ToString() + "' ,INTERVAL 7 DAY)";
                    }
                    else
                    {
                        renewal_fulfillment_delay = "DATE_ADD('" + date_purchased.ToString() + "' ,INTERVAL 84 DAY)";
                    }

                    fulfillment_delay_batch_week_array = new DataTable();
                    fulfillment_delay_batch_week_array = get_fulfillment_batch_week(renewal_fulfillment_delay);
                    fulfillment_delay_batch_week = (from DataRow dr in fulfillment_delay_batch_week_array.Rows select (string)dr["fulfillment_batch_week"]).FirstOrDefault();
                    fulfillment_delay_batch_date = Convert.ToDateTime((from DataRow dr in fulfillment_delay_batch_week_array.Rows select (DateTime)dr["fulfillment_batch_date"]).FirstOrDefault());

                    fulfillment_current_batch_week_array = new DataTable();
                    fulfillment_current_batch_week_array = get_fulfillment_batch_week();
                    fulfillment_current_batch_week = (from DataRow dr in fulfillment_current_batch_week_array.Rows select (string)dr["fulfillment_batch_week"]).FirstOrDefault();
                    fulfillment_current_batch_date = Convert.ToDateTime((from DataRow dr in fulfillment_current_batch_week_array.Rows select (DateTime)dr["fulfillment_batch_date"]).FirstOrDefault());

                    //if this is a renewals FULFILL batch item, we should always get the delay batch week, unless
                    //we've already gone past it.
                    if (fulfillment_status_id == FULFILLMENT_FULFILL_ID && Convert.ToInt32(fulfillment_current_batch_week) <= Convert.ToInt32(fulfillment_delay_batch_week))
                    {
                        fulfillment_batch_week = (from DataRow dr in fulfillment_delay_batch_week_array.Rows select (string)dr["fulfillment_batch_week"]).FirstOrDefault();
                        fulfillment_batch_date = Convert.ToDateTime((from DataRow dr in fulfillment_delay_batch_week_array.Rows select (DateTime)dr["fulfillment_batch_date"]).FirstOrDefault());
                    }
                    else
                    {
                        //For the CANCEL batch item we use the current, because this is only a PENDING -> CANCEL
                        fulfillment_batch_week = (from DataRow dr in fulfillment_current_batch_week_array.Rows select (string)dr["fulfillment_batch_week"]).FirstOrDefault();
                        fulfillment_batch_date = Convert.ToDateTime((from DataRow dr in fulfillment_current_batch_week_array.Rows select (DateTime)dr["fulfillment_batch_date"]).FirstOrDefault());
                    }

                    fulfillment_batch_id = get_fulfillment_batch_id(products_id, fulfillment_status_id, fulfillment_batch_week, fulfillment_batch_date, skus_type, skus_type_order, skus_type_order_period);
                    sql_data_array2 = new Dictionary<string, object>();
                    sql_data_array2["date_added"] = "now()";
                    sql_data_array2["fulfillment_batch_id"] = fulfillment_batch_id;
                    sql_data_array2["orders_id"] = orders_id;
                    string batch_items_query = tep_db_perform(TABLE_FULFILLMENT_BATCH_ITEMS, sql_data_array2);
                    command5 = new MySqlCommand(batch_items_query, myConn);
                    command5.ExecuteNonQuery();    
                    fulfillment_batch_items_id = Convert.ToInt32(command5.LastInsertedId);
                    command5.Dispose();

                //this renewal process only cancel's PENDING orders, so we don't need to check
                //the batch week.so add another record for ignore fulfillment.
                    if (fulfillment_status_id == FULFILLMENT_CANCEL_ID)
                    {
                        if (orders_previous_status == DEFAULT_ORDERS_STATUS_ID)
                        {
                            fulfillment_batch_id = get_fulfillment_batch_id(products_id, FULFILLMENT_IGNORE_FULFILLMENT_ID, fulfillment_batch_week, fulfillment_batch_date, skus_type, skus_type_order, skus_type_order_period);
                            sql_data_array = new Dictionary<string, object>();
                            sql_data_array["date_added"] = "now()";
                            sql_data_array["fulfillment_batch_id"] = fulfillment_batch_id;
                            sql_data_array["orders_id"] = orders_id;
                            string batch_items_query2 = tep_db_perform(TABLE_FULFILLMENT_BATCH_ITEMS, sql_data_array);
                            command5 = new MySqlCommand(batch_items_query2, myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                    }
                    else if (fulfillment_status_id == FULFILLMENT_CHANGE_ADDRESS_ID)
                    {
                        //if this is an address change we need to check to see if this is in the same batch week
                        //if so, we need to enter the fulfillment_change_address batch item and also
                        //add back in the batch item for whatever batch_item was there previous.
                        //use the orders' fulfillment_batch_id to see if we're in the same batchweek.
                        if (order_fulfillment_batch_id.ToString() != string.Empty)
                        {
                            command5 = new MySqlCommand("select fb.fulfillment_status_id, fb.fulfillment_batch_week from " + TABLE_FULFILLMENT_BATCH + " fb where fb.fulfillment_batch_id = '" + order_fulfillment_batch_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            MySqlDataReader order_fulfillment_batch_array;

                            order_fulfillment_batch_array = command5.ExecuteReader();
                            while (order_fulfillment_batch_array.Read())
                            {
                                order_fulfillment_batch_week = order_fulfillment_batch_array["fulfillment_batch_week"].ToString();
                                order_fulfillment_status_id = Convert.ToInt32(order_fulfillment_batch_array["fulfillment_status_id"]);
                            }

                            order_fulfillment_batch_array.Close();
                            command5.Dispose();

                            //if we are in the same batchweek as the most recent order fulfillment batch then add
                            //that batch back in, except if the previous status was a change address.
                            //we are checking to see if the $order_fulfillment_batch_week is greater or equal to the
                            //current batch week, because a renewal might have a delayed fulfillment batch item.
                            //so if it is the same batch week or if the batch week is in the future then
                            //add back in that batch week.
                            if (order_fulfillment_status_id != Convert.ToInt32(FULFILLMENT_CHANGE_ADDRESS_ID) && Convert.ToInt32(order_fulfillment_batch_week) >= Convert.ToInt32(fulfillment_batch_week))
                            {
                                sql_data_array = new Dictionary<string, object>();
                                sql_data_array["date_added"] = "now()";
                                sql_data_array["fulfillment_batch_id"] = order_fulfillment_batch_id;
                                sql_data_array["date_added"] = orders_id;
                                string batch_items_query2 = tep_db_perform(TABLE_FULFILLMENT_BATCH_ITEMS, sql_data_array);
                                command5 = new MySqlCommand(batch_items_query2, myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();
                                update_orders_fulfillment_status_id = false;
                            }
                            else
                            {
                                //If there is no $order_fulfillment_batch_id, add in the ignore fulfillment.
                                //This means an address change was done on a Pending order.
                                fulfillment_batch_id = get_fulfillment_batch_id(products_id, FULFILLMENT_IGNORE_FULFILLMENT_ID, fulfillment_batch_week, fulfillment_batch_date, skus_type, skus_type_order, skus_type_order_period);
                                sql_data_array = new Dictionary<string, object>();
                                sql_data_array["date_added"] = "now()";
                                sql_data_array["fulfillment_batch_id"] = fulfillment_batch_id;
                                sql_data_array["date_added"] = orders_id;
                                string batch_items_query2 = tep_db_perform(TABLE_FULFILLMENT_BATCH_ITEMS, sql_data_array);
                                command5 = new MySqlCommand(batch_items_query2, myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();
                            }
                            //Add in the previous_address_book_id to the batch_item. Used to display on orders.
                            command5 = new MySqlCommand("update fulfillment_batch_items fbi, orders o set fbi.previous_delivery_address_book_id = o.previous_delivery_address_book_id where fbi.orders_id = o.orders_id and fbi.fulfillment_batch_items_id = '" + fulfillment_batch_items_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();
                        }
                    }

                        //In most cases we need to update the order to reflect the newly created batch_id
                        //but with address change within same batch week we don't have to since
                        //the last status should revert back to the original. So we can save an update here.
                        if (update_orders_fulfillment_status_id)
                        {
                            //only if this is a fulfill status go ahead and set the right delivery_range and renewal notice date.
                            //this is done here since renewals are delayed and might not get paid when we charge them (track2)
                            //so when we call this function with a fulfill_id we konw it was paid and ready for fulfillment.

                            update_fulfill_fields = string.Empty;
                            if (fulfillment_status_id == FULFILLMENT_FULFILL_ID)
                            {
                                //Update the orders' end_delivery_range date
                                //also set the renew_date.
                                //also update the order's renewal_transactions_date so it won't get pulled again.
                                //this is being added in the update stmt below.
                                update_fulfill_fields = ", renewal_transaction_date = null, end_delivery_range = '" + get_end_delivery_range(first_issue_delay_days) + "', renewal_date='" + get_renewal_date(orders_id) + "' ";
                            }

                            //Now add the last status to the order.fulfillment_batch_id field. Used in fulfillment process.
                            //this will always hold the most recent fulfillment status.
                            command5 = new MySqlCommand("update " + TABLE_ORDERS + " set fulfillment_batch_id = '" + fulfillment_batch_id.ToString() + "'" + update_fulfill_fields + " where orders_id = '" + orders_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();

                            //The problem with the above is that it overwrites a previous batchweek's fulfillment_batch_id this causes us to not be able to
                            //rerun a run monday report and getting the same result. Therefore we need to make sure we do this by batch week and store that in separate table.
                            //I'll keep doing the previous since a lot of code it report are touching it.
                            //I am using the handy dandy replace since I have a unique index on orders and fulfillment_batch_week. This will always give me the latest
                            //batch_id for a particular batchweek.
                            command5 = new MySqlCommand("replace into " + TABLE_ORDERS_FULFILLMENT_BATCH_HISTORY + " (orders_id, fulfillment_batch_id, fulfillment_batch_week) values ('" + orders_id.ToString() + "', '" + fulfillment_batch_id + "', '" + fulfillment_batch_week + "')", myConn);
                            command5.ExecuteNonQuery();
                            command5.Dispose();

                            //update the products_ordered field for bestsellers
                            command5 = new MySqlCommand("select * from " + TABLE_ORDERS + " where orders_id ='" + orders_id.ToString() + "'", myConn);
                            command5.ExecuteNonQuery();
                            MySqlDataReader orders_paid_fetch;
                            orders_paid_fetch = command5.ExecuteReader();

                            while (orders_paid_fetch.Read())
                            {
                                paid_status = orders_paid_fetch["orders_status"].ToString();
                            }

                            orders_paid_fetch.Close();
                            command5.Dispose();

                            if (paid_status == "2")
                            {
                                command5 = new MySqlCommand("update " + TABLE_PRODUCTS + " set products_ordered = products_ordered + 1 where products_id = '" + products_id.ToString() + "'", myConn);
                                command5.ExecuteNonQuery();
                                command5.Dispose();
                            }
                        }        
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static int get_fulfillment_batch_id(int productsId, string fulfillmentStatusId, string fulfillmentBatchWeek, DateTime fulfillmentBatchDate, string skusType, int skusTypeOrder, int skusTypeOrderPeriod)
        {
            try
            {
                //Because of potential transactional problems, we can't just do a search and then an insert, because
                //another thread might have inserted in between our select and insert. Locking won't work either because
                //we are using different threads each time we call tep_db_query. So I will just insert and the
                //newly added tep_db_query_return_error function will allow the insert to fail, but continue the script.
                // we can then check for error using tep_db_query_returned_error.
                int fulfillmentBatchId = 0;
                bool mySqlError = false;
                MySqlDataReader bId;

                try
                {
                    command5 = new MySqlCommand("insert into " + TABLE_FULFILLMENT_BATCH + @" (date_added, fulfillment_batch_week, fulfillment_batch_date, 
                fulfillment_status_id, products_id, skus_type, skus_type_order, skus_type_order_period)
                values (now(), '" + fulfillmentBatchWeek + "', '" + fulfillmentBatchDate.ToString() + "', '" + fulfillmentStatusId + "', '" + productsId.ToString() +
                    "', '" + skusType + "', '" + skusTypeOrder.ToString() + "', '" + skusTypeOrderPeriod.ToString() + "')", myConn);
                    command5.ExecuteNonQuery();
                    command5.Dispose();
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine(ex.Message);
                    if (ex.Message.Contains("Duplicate"))
                    {
                        mySqlError = true;
                    }
                }

                if (mySqlError)
                {
                    //assuming duplicate error, so select batch_id from existing record.
                    command5 = new MySqlCommand(@"select fulfillment_batch_id from " + TABLE_FULFILLMENT_BATCH + " where products_id = '" + productsId.ToString() +
                        "' and fulfillment_status_id = '" + fulfillmentStatusId + "' and fulfillment_batch_week = '" + fulfillmentBatchWeek +
                    "' and skus_type ='" + skusType + "' and skus_type_order = '" + skusTypeOrder.ToString() + "' and skus_type_order_period = '"
                        + skusTypeOrderPeriod.ToString() + "'", myConn);
                    command5.ExecuteNonQuery();
                    bId = command5.ExecuteReader();
                    while (bId.Read())
                    {

                        fulfillmentBatchId = Convert.ToInt32(bId["fulfillment_batch_id"]);
                    }
                    bId.Close();
                    command5.Dispose();
                }
                else
                {
                    //no error,so get the new id.
                    fulfillmentBatchId = Convert.ToInt32(command5.LastInsertedId);
                }
                return fulfillmentBatchId;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private static bool isPerfectRenewal(Dictionary<string, object> order)
        {
            try
            {
                if (order["cc_number"].ToString() == string.Empty || order["cc_expires"].ToString() == string.Empty)
                {
                    if (order["renewal_cc_number"].ToString() == string.Empty ||
                        order["renewal_cc_expires"].ToString() == string.Empty)
                    {
                        return false;
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }

        private static void debug(Dictionary<string, object> array, string name)
        {
            try
            {
                if (Debug != true)
                    return;

                foreach (KeyValuePair<string, object> KV in array)
                {
                    var my_array = array[KV.Key];

                    if (my_array == typeof(KeyValuePair<,>))
                    {
                        //my_array = new Dictionary<string, object>();
                        Console.WriteLine(my_array.ToString() + " is a key,value pair.");
                        //debug(my_array, name + "[" + KV.Key + "]");
                    }
                    else
                    {
                        Console.WriteLine(name + "[" + KV.Key + "]=" + array[KV.Key].ToString() + "\n");
                    }
                }
                Console.WriteLine("\n");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static string tep_db_perform(string table, Dictionary<string, object> data, string action = "insert", string parameters = "", string link = "db_link")
        {
            try
            {
                string val;
                if (action == "insert")
                {
                    query = "insert into " + table + "(";

                    foreach (KeyValuePair<string, object> col in data)
                    {
                        query += col.Key + ", ";
                    }

                    query = query.Substring(0, query.Length - 2) + ") values (";
                    foreach (KeyValuePair<string, object> col in data)
                    {
                        val = col.Value.ToString();

                        switch (val)
                        {
                            case "null":
                                query += "null, ";
                                break;
                            default:
                                //if there is a function related to now(), assume it doesn't need
                                //quotes.

                                if (val.IndexOf("now()") == -1)
                                {
                                    query += "\'" + val.Replace("'", "\\'") + "\', ";
                                }
                                else
                                {
                                    query += val.Replace("'", "\\'") + ", ";
                                }
                                break;
                        }
                    }
                    query += query.Substring(0, query.Length - 2) + ")";
                }
                else if (action == "replace")
                {
                    query = "replace into " + table + " (";
                    foreach (KeyValuePair<string, object> col in data)
                    {
                        query += col.Key + ", ";
                    }
                    query = query.Substring(0, query.Length - 2) + ") values (";

                    foreach (KeyValuePair<string, object> col in data)
                    {
                        val = col.Value.ToString();

                        switch (val)
                        {
                            case "now()":
                                query += "now(), ";
                                break;
                            case "null":
                                query += "null, ";
                                break;
                            default:
                                query += "\'" + val.Replace("'", "\\'") + "\', ";
                                break;
                        }
                    }
                    query += query.Substring(0, query.Length - 2) + ")";
                }
                else if (action == "update")
                {
                    query = "update " + table + " set ";
                    foreach (KeyValuePair<string, object> col in data)
                    {
                        val = col.Value.ToString();

                        switch (val)
                        {
                            case "now()":
                                query += col.Key + " = now(), ";
                                break;
                            case "null":
                                query += col.Key + "= null, ";
                                break;
                            default:
                                query += col.Key + " = \'" + val.Replace("'", "\\'") + "\', ";
                                break;
                        }
                    }
                    query += query.Substring(0, query.Length - 2) + " where " + parameters;
                }
                return query;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        private static string get_currency_format(string number, string currency_type)
        {
            try
            {
                string currency_format = string.Empty;
                //get the currencies
                var results = (from m in currencies.AsEnumerable()
                               where m.Field<string>("code") == currency_type
                               select m).FirstOrDefault();

                string decimalPoint = results["decimal_point"].ToString();
                string thousandsPoint = results["thousands_point"].ToString();
                string decimalPlaces = new String('0', Convert.ToInt32(results["decimal_places"]));
                string getRoundedNum = tep_round(Convert.ToDouble(number) * Convert.ToInt32(results["value"]),
                    Convert.ToInt32(results["decimal_places"])).ToString();
                getRoundedNum = Convert.ToDecimal(getRoundedNum).ToString("#,##0." + decimalPlaces);
                getRoundedNum = getRoundedNum.Replace(",", thousandsPoint);
                int lastDecimal = Convert.ToInt32(getRoundedNum.LastIndexOf("."));
                getRoundedNum = getRoundedNum.Remove(lastDecimal, 1).Insert(lastDecimal, decimalPoint);

                currency_format = results["symbol_left"].ToString() +
                                   getRoundedNum + results["symbol_right"].ToString();
                return currency_format;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        private static DataTable get_fulfillment_batch_week(string compareDate = "")
        {
            try
            {
                if (compareDate == string.Empty)
                {
                    compareDate = "now()";
                }

                command = new MySqlCommand(string.Empty, myConn);
                command.CommandText = @"SELECT fulfillment_batch_date, fulfillment_batch_week
                    				  FROM fulfillment_batch_week
                    				  WHERE to_days( fulfillment_batch_date )  >= to_days(" + compareDate + @")
                    			      ORDER  BY fulfillment_batch_date ASC
                    				  LIMIT 1";
                command.ExecuteNonQuery();
                DataTable batch_week = new DataTable();
                using (MySqlDataAdapter da = new MySqlDataAdapter(command))
                {
                    da.Fill(batch_week);
                }
                return batch_week;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        // Wrapper function for round()
        private static double tep_round(double number, int precision)
        {
            try
            {
                int num = number.ToString().Substring(number.ToString().IndexOf(".") + 1).Length;
                if (number.ToString().IndexOf(".") != -1 && num > precision)
                {
                    number = Convert.ToDouble(number.ToString().Substring(0, number.ToString().IndexOf(".") + 1 + 1 + precision));

                    if (Convert.ToInt32(number.ToString().Substring(number.ToString().Length - 1)) >= 5)
                    {
                        if (precision > 1)
                        {
                            string numbr = number.ToString().Substring(0, number.ToString().Length - 1).ToString();
                            string rep = new String('0', precision - 1);
                            string zero = "0.";
                            string one = "1";
                            string combine = zero + rep + one;
                            number =
                                Convert.ToDouble(numbr) + Convert.ToDouble(combine);
                        }
                        else if (precision == 1)
                        {
                            number =
                                Convert.ToDouble(number.ToString().Substring(0, number.ToString().Length - 1).ToString()) +
                                0.1;
                        }
                        else
                        {
                            number =
                                Convert.ToDouble(number.ToString().Substring(0, number.ToString().Length - 1).ToString()) +
                                1;
                        }
                    }
                    else
                    {
                        number =
                            Convert.ToDouble(number.ToString().Substring(0, number.ToString().Length).ToString());
                    }
                }
                return number;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        // For expiration dates. Added September 2012 (MCS)
        private static bool is_date_stale(string exp_month, string exp_year)
        {
            try
            {
                bool stale = false;
                double t1 =
                    DateTime.Parse("1/" + exp_month + "/" + exp_year).Subtract(new DateTime(1970, 2, 1)).TotalSeconds;
                string t2 = DateTime.Parse("2" + "/1/" + "1970").AddSeconds(t1).ToString("dd/MM/yyyy");
                double t3 = DateTime.Parse(t2).Subtract(new DateTime(1970, 2, 1)).TotalSeconds;
                stale = DateTime.Now.Subtract(new DateTime(1970, 2, 1)).TotalSeconds - t3 > 0 ? true : false;

                return stale;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }

        private static string get_key()
        {
            try
            {
                // step 1, calculate MD5 hash from input
                System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create();
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(Key);
                byte[] hash = md5.ComputeHash(inputBytes);

                // step 2, convert byte array to hex string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hash.Length; i++)
                {
                    sb.Append(hash[i].ToString("X2"));
                }
                return sb.ToString();
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        private static string get_key(string ky)
        {
            try
            {
                // step 1, calculate MD5 hash from input
                System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create();
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(ky);
                byte[] hash = md5.ComputeHash(inputBytes);

                // step 2, convert byte array to hex string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hash.Length; i++)
                {
                    sb.Append(hash[i].ToString("X2"));
                }
                return sb.ToString();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }
        private static string get_pfp_pwd()
        {
            try
            {
                string key = get_key();
                string input = MODULE_PAYMENT_PAYFLOWPRO_PWD;
                input = input.Replace("\n", "");
                input = input.Replace("\t", "");
                input = input.Replace("\r", "");
                input = input.Trim();
                byte[] bites = System.Convert.FromBase64String(input);
                input = System.Text.Encoding.UTF8.GetString(bites);
	           /*    $td = mcrypt_module_open ('tripledes', '', 'ecb', '');
	            $key = substr(md5($key),0,mcrypt_enc_get_key_size ($td));
	            $iv = mcrypt_create_iv (mcrypt_enc_get_iv_size ($td), MCRYPT_RAND);
	            mcrypt_generic_init ($td, $key, $iv);
	            $decrypted_data = mdecrypt_generic ($td, $input);
	            mcrypt_generic_deinit ($td);
	            mcrypt_module_close ($td);
	            return trim(chop($decrypted_data)); */
                return key;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        //$input - stuff to decrypt
        //$key - the secret key to use
        private static string decrypt_cc(int input, int customer_id)
        {
            try
            {
                string key = get_key();
                string input2 = input.ToString();
                input2 = input2.Replace("\n", "");
                input2 = input2.Replace("\t", "");
                input2 = input2.Replace("\r", "");
                input2 = input2.Trim();
                byte[] bites = System.Convert.FromBase64String(input2);
                input2 = System.Text.Encoding.UTF8.GetString(bites);
                /*    $td = mcrypt_module_open ('tripledes', '', 'ecb', '');
                 $key = substr(md5($key),0,mcrypt_enc_get_key_size ($td));
                 $iv = mcrypt_create_iv (mcrypt_enc_get_iv_size ($td), MCRYPT_RAND);
                 mcrypt_generic_init ($td, $key, $iv);
                 $decrypted_data = mdecrypt_generic ($td, $input);
                 mcrypt_generic_deinit ($td);
                 mcrypt_module_close ($td);
                 return trim(chop($decrypted_data)); */

                return key;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        private static string encrypt_cc(int input, int customer_id)
        {
            try
            {
                string key = get_key();
                string input2 = input.ToString();
                input2 = input2.Replace("\n", "");
                input2 = input2.Replace("\t", "");
                input2 = input2.Replace("\r", "");
                input2 = input2.Trim();

                /*      $td = mcrypt_module_open ('tripledes', '', 'ecb', '');
                        $key = substr(md5($key),0,mcrypt_enc_get_key_size ($td));
                        $iv = mcrypt_create_iv (mcrypt_enc_get_iv_size ($td), MCRYPT_RAND);
                        mcrypt_generic_init ($td, $key, $iv);
                        $encrypted_data = mcrypt_generic ($td, $input);
                        mcrypt_generic_deinit ($td);
                        mcrypt_module_close ($td);
                        return trim(chop(base64_encode($encrypted_data)));
                 
                 */
                return key;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        private static string get_merchant_processor_reporting_group(int skinsitesId)
        {
            try
            {
                  string ssId = skinsitesId.ToString().Replace("'", "\\'");
                  string qur = "select merchant_processor_reporting_group from " + TABLE_SKINSITES + " where skinsites_id = '" + ssId + "' ";

                  // Build query to retireve merchant processor reporting group.
                  command = new MySqlCommand(qur, myConn);
                  command.ExecuteNonQuery();

                  MySqlDataReader myReader;
                  myReader = command.ExecuteReader();
                  if (myReader.HasRows)
                  {
                      while (myReader.Read())
                      {
                          return myReader["merchant_processor_reporting_group"].ToString();
                      }
                  }
                  else
                  {
                      return "";
                  }
                return "";
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
        }

        private static Dictionary<string, object> pfpro_process(Dictionary<string, object> trans, string hostAddress, string port = "", string timeout = "",
                                  string proxy_url = "", string proxy_port = "", string proxy_logon = "",
                                  string proxy_password = "")
        {
            try
            {
                resp = new Dictionary<string, object>();
                hostAddress = pfpro_defaulthost;
                port = pfpro_defaultport;
                timeout = pfpro_defaulttimeout;
                proxy_url = pfpro_proxyaddress;
                proxy_port = pfpro_proxyport;
                proxy_logon = pfpro_proxylogin;
                proxy_password = pfpro_proxypassword;

                Environment.SetEnvironmentVariable("LD_LIBRARY_PATH", LD_LIBRARY_PATH_ENV, EnvironmentVariableTarget.Machine);

                if (transaction.Count == 0)
                    return null;

                return resp;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        private static DataTable get_configuration_values()
        {
            try
            {
                 DataTable configuration_values = null;
                 command = new MySqlCommand("select configuration_key, configuration_value from configuration", myConn);
                 command.ExecuteNonQuery();
                 configuration_values = new DataTable();
                 using (MySqlDataAdapter da = new MySqlDataAdapter(command))
                 {
                     da.Fill(configuration_values);
                 }
                return configuration_values;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        private static void set_config_values(DataTable config_dt)
        {
            try
            {
                DEFAULT_ORDERS_STATUS_ID = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "DEFAULT_ORDERS_STATUS_ID" select (string)dr["configuration_value"]).FirstOrDefault();
                LANGUAGE_CURRENCY = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "LANGUAGE_CURRENCY" select (string)dr["configuration_value"]).FirstOrDefault();
                DEFAULT_CURRENCY = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "DEFAULT_CURRENCY" select (string)dr["configuration_value"]).FirstOrDefault();
                USE_DEFAULT_LANGUAGE_CURRENCY = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "USE_DEFAULT_LANGUAGE_CURRENCY" select (string)dr["configuration_value"]).FirstOrDefault();
                RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "RENEWAL_POSTCARD_CONFIRMATION_DELAY_DAYS" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_USER = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_USER" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_VENDOR = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_VENDOR" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_PARTNER = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PARTNER" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_TRXTYPE = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_TRXTYPE" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_TENDER = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_TENDER" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_PWD = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PWD" select (string)dr["configuration_value"]).FirstOrDefault();
                MODULE_PAYMENT_PAYFLOWPRO_PFPRO_CERT_PATH_ENV = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PFPRO_CERT_PATH_ENV" select (string)dr["configuration_value"]).FirstOrDefault();
                MAX_RENEWAL_CREDIT_CARD_CHARGE_ATTEMPTS = Convert.ToInt32((from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MAX_RENEWAL_CREDIT_CARD_CHARGE_ATTEMPTS" select (string)dr["configuration_value"]).FirstOrDefault());
                MAX_RENEWAL_CREDIT_CARD_CHARGE_EXPIRATION_FAILURES = Convert.ToInt32((from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MAX_RENEWAL_CREDIT_CARD_CHARGE_EXPIRATION_FAILURES" select (string)dr["configuration_value"]).FirstOrDefault());
                DEFAULT_RETRY_RENEWAL_CHARGE_DAYS = Convert.ToInt32((from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "DEFAULT_RETRY_RENEWAL_CHARGE_DAYS" select (string)dr["configuration_value"]).FirstOrDefault());
                MODULE_PAYMENT_PAYFLOWPRO_PFPRO_ORDER_STATUS_ID = Convert.ToInt32((from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PFPRO_ORDER_STATUS_ID" select (string)dr["configuration_value"]).FirstOrDefault());
                SEND_EMAILS = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "SEND_EMAILS" select (string)dr["configuration_value"]).FirstOrDefault();
                DEFAULT_RENEWAL_LEADTIME = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "DEFAULT_RENEWAL_LEADTIME" select (string)dr["configuration_value"]).FirstOrDefault(); ;
                pfpro_defaulthost = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_HOSTADDRESS" select (string)dr["configuration_value"]).FirstOrDefault();
                pfpro_defaultport = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_HOSTPORT" select (string)dr["configuration_value"]).FirstOrDefault();
                pfpro_defaulttimeout = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_TIMEOUT" select (string)dr["configuration_value"]).FirstOrDefault();
                pfpro_proxyaddress = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PROXY_ADDRESS" select (string)dr["configuration_value"]).FirstOrDefault();
                pfpro_proxyport = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PROXY_PORT" select (string)dr["configuration_value"]).FirstOrDefault();
                pfpro_proxylogin = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PROXY_LOGON" select (string)dr["configuration_value"]).FirstOrDefault();
                pfpro_proxypassword = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PROXY_PASSWORD" select (string)dr["configuration_value"]).FirstOrDefault();
                PFPRO_EXE_PATH = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PFPRO_EXE_PATH_D" select (string)dr["configuration_value"]).FirstOrDefault();
                LD_LIBRARY_PATH_ENV = (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_LD_LIBRARY_PATH_ENV" select (string)dr["configuration_value"]).FirstOrDefault();
                PFPRO_CERT_PATH_ENV = "PFPRO_CERT_PATH=" + (from DataRow dr in config_dt.Rows where (string)dr["configuration_key"] == "MODULE_PAYMENT_PAYFLOWPRO_PFPRO_CERT_PATH_ENV" select (string)dr["configuration_value"]).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static string get_end_delivery_range(int firstIssueDelayDays)
        {
            try
            {
                fulfillment_delay_batch_week_array = new DataTable();
                fulfillment_delay_batch_week_array = get_fulfillment_batch_week(renewal_fulfillment_delay);
               
                fulfillment_delay_batch_date = Convert.ToDateTime((from DataRow dr in fulfillment_delay_batch_week_array.Rows select (DateTime)dr["fulfillment_batch_date"]).FirstOrDefault());
                if (first_issue_delay_days > 0)
                {
                    int yearf = Convert.ToInt32(fulfillment_delay_batch_date.ToString().Substring(0,4));
                    int monthf = Convert.ToInt32(fulfillment_delay_batch_date.ToString().Substring(5, 2));
                    int dayf = Convert.ToInt32(fulfillment_delay_batch_date.ToString().Substring(8, 2));
                    int hourf = Convert.ToInt32(fulfillment_delay_batch_date.ToString().Substring(11, 2));
                    int minutef = Convert.ToInt32(fulfillment_delay_batch_date.ToString().Substring(14, 2));
                    int secondf = Convert.ToInt32(fulfillment_delay_batch_date.ToString().Substring(17, 2));

                    //Calcuate the last day of the expected first issue date range.
                    //The batch_date + 3 gets to Wednesday when orders are sent for fulfillment.
                    //The first day of the range is the fulfillment day + the delay days.
                    //10 days is added for the last day of the date range.
                    DateTime origin = new DateTime(yearf, monthf, dayf, hourf, minutef, secondf);
                    DateTime origin2 = new DateTime(1970, 1, 1);
                    origin = origin.AddSeconds(86400 * (first_issue_delay_days + 13));
                    TimeSpan end_delivery_range_date = origin - origin2;
                    int totSeconds = Convert.ToInt32(end_delivery_range_date.TotalSeconds);
                    origin2 = origin2.AddSeconds(totSeconds);
                    return origin2.ToString("yyyy-MM-dd HH:mm:ss");
                }
                else
                {
                    return string.Empty;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return string.Empty;
            }
        }

        private static string get_template_dir(int SkinsitesId)
        {
            try
            {
                string _template_dir = (from DataRow dr in skinsites.Rows where Convert.ToInt32(dr["skinsites_id"]) == SkinsitesId select (string)dr["tplDir"]).FirstOrDefault();
                return _template_dir;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return string.Empty;
            }
        }
    }
}
