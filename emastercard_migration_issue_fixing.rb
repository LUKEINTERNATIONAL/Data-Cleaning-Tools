require 'csv'
require 'mysql2'
require 'yaml'
def start
  site_code = PatientIdentifier.where(identifier_type: 4).first.identifier.split("-").first.downcase
  path = "../eMastercard2Nart/tmp/#{site_code}-patients-with-outcome-on-first-visit.csv"

  patient_with_outcomes_on_first_visit = CSV.parse(File.read(path), headers: true).reject{|record|
                                                       record["ARV Number"].blank? or record["Outcome"]=="Blank"}
  #patient_without_visits = CSV.parse(File.read(path2), headers: true).reject{|record| record["ARV Number"].blank?}
  @connector = old_emastercard_database
  puts "Let the magic begin!!"
  process(patient_with_outcomes_on_first_visit,"patients with outcomes on first visit",site_code)

  puts "-----------------------------------------------------------------------------------------\n"
  puts "Now performing other checks"
  #create some temporary tables
  end_date = "2020-03-31".to_date
  create_tmp_patient_table
  #load the data
  load_data_into_temp_earliest_start_date(end_date)
  #loading outcomes
  update_cum_outcome(end_date)
  #get outcomes
  patient_ids = get_patients_with_outcomes.map{|pat| pat["patient_id"]}
  patient_arv_ids = get_patient_identifiers(patient_ids)
  missing_outcomes = get_patient_with_outcomes_inside_visit(patient_arv_ids)
  unless missing_outcomes.blank?
      puts missing_outcomes
      puts "#{missing_outcomes.length} additional patients to be processed out of #{patient_arv_ids.length} patients"
      process(missing_outcomes,"patients with outcomes inside visits",site_code)
  end
  #recheck the missing ARVS
  #loading outcomes
  missing_patient_finder(site_code,end_date)
end

def process(records,info,site_code)
    puts "=====#{records.length} patient records to be fixed==="
    @patient_ids = []
    records = records.reject{|record| record["ARV Number"].blank? or record["Outcome"]=="Blank"}

    records.each do |record|
      #patient_details
      arv_identifier = site_code+"-ARV-"+record["ARV Number"].to_s
      identifier = record["ARV Number"]
      visit_date = record["Outcome Date"].to_date
      date = visit_date
      puts arv_identifier.upcase
      patient_id = PatientIdentifier.where(identifier: arv_identifier.upcase).first.patient_id
      @patient_ids << patient_id
      gender = patient_gender(patient_id)
      age = patient_age(patient_id,visit_date)
      outcome = record["Outcome"]

      #create encounters and observations
      reception(patient_id,visit_date,age)
      vitals(patient_id,date,gender,age)
      consultation(patient_id,date)
      treatment(patient_id,date,gender,age,outcome,identifier)
    end
    #Use this chance to destroy all ambiguous default outcomes in patient state tabe
    pat_states = PatientState.where(state: 12).destroy_all
    puts "#{@patient_ids.length} { #{info}} have been sucessfully fixed \n"
end

def reception(patient_id,date,age)
  puts "Now create reception encounter for patient: #{patient_id}"
  encounter_id = create_encounter(patient_id,51,date)
  #patient present
  obs_value_coded(patient_id,encounter_id,1805,date,1065)
  if age > 12
      obs_value_coded(patient_id,encounter_id,2122,date,1066)
  else
    obs_value_coded(patient_id,encounter_id,2122,date,1065)
  end
end

def vitals(patient_id,date,gender,age)
  puts "Now create reception vitals for patient: #{patient_id}"
  weight,height = 0,0
  #generalize weight and height
  if age >= 14 and gender=="F"
    weight = 55
    height = 158
  elsif age >= 14 and gender == "M"
    weight = 60
    height = 160
  elsif age >= 8 and age <=13
    weight = 30
    height =140
  elsif age >= 4 and age < 8
     weight = 20
     height = 120
  elsif age >=2 and age < 4
     weight = 10
     height = 110
  else
    weight = 5
    height = 90
  end
 #create vitals encounter
 encounter_id = create_encounter(patient_id,6,date)
 #create observations for weight and height
  obs_value_numeric(patient_id,encounter_id,5089,date,weight)
  #height
  obs_value_numeric(patient_id,encounter_id,5090,date,height)
end

def consultation(patient_id,date)
   encounter_id = create_encounter(patient_id,53,date)
   obs_value_coded(patient_id,encounter_id,7755,date,1067)
   obs_value_coded(patient_id,encounter_id,7459,date,7454)
end

def treatment(patient_id,date,gender,age,outcome,arv_identifier)
  #initualize regimen with what is old emastercard
  #regimen = get_regimen_from_old_db(arv_identifier)
  regimen = get_regimen_from_old_db(arv_identifier)
  puts regimen
  if regimen.blank?
        if date.year < 2012 and age > 12
          regimen = "1A"
        elsif (2012..2018).to_a.include?(date.year) and age > 12
          regimen = "5A"
        elsif (2019..2020).to_a.include?(date.year) and age > 12 and gender == "M"
          regimen = "13A"
        elsif (2019..2020).to_a.include?(date.year) and age > 12 and gender == "F"
          regimen = "5A"
        elsif (2019..2020).to_a.include?(date.year) and age > 50 and gender == "F"
          regimen = "13A"
        elsif age < 12
          regimen = "2P"
        else
          regimen = "Other"
        end
  end
  regimen = regimen == "1P" ? "2P":regimen #convert legacy legimen
  puts "Now create treatment encounter for patient: #{patient_id}"
  encounter_id = create_encounter(patient_id,25,date)
  #create drug order
  drug_ids = []

  if regimen != "1A"
    drug_ids = MohRegimenLookup.where(regimen_name: regimen).map(&:drug_inventory_id)
  else
    drug_ids = [613]
  end

  drug_ids.each do |drug_id|
    drug_concept = Drug.find(drug_id).concept_id
    order_id = create_order(patient_id,encounter_id,drug_concept,date)
    #create drug order
    drug_order = create_drug_order(order_id,drug_id)
  end
  puts "Now creating treatment obsevation for patient: #{patient_id}"
  obs_value_text(patient_id,encounter_id,8375,date,regimen)
 #Now do the dispensing
  dispensing(patient_id,date,gender,age,regimen,outcome)
end

def create_order(patient_id,encounter_id,drug_concept_id,date)
  order_id = Order.create(:order_type_id=>1,
                          :concept_id=> drug_concept_id,
                          :orderer=>1,:encounter_id=> encounter_id,
                          :start_date=> date,
                          :auto_expire_date=>nil,
                          :patient_id =>patient_id,
                          :creator=>1
                         ).id
end

def create_drug_order(order_id,drug_id)
   drug_order = DrugOrder.new()
   drug_order.order_id=order_id
   drug_order.drug_inventory_id=drug_id
   drug_order.equivalent_daily_dose = Drug.find(drug_id).dose_strength rescue 0
   drug_order.quantity=30
   drug_order.save!
end

def dispensing(patient_id,date,gender,age,regimen,outcome)
   puts "Now creating dispensation encounter for patient: #{patient_id}"

  if regimen != "1A"
     drug_ids = MohRegimenLookup.where(regimen_name: regimen).map(&:drug_inventory_id)
  else
     drug_ids = [613]
  end
  #create dispensing encounter
  encounter_id = create_encounter(patient_id,54,date)
  drug_ids.each do |drug_id|
     #put default value of 30 pills
     obs_value_drug_numeric(patient_id,encounter_id,2834,date,drug_id,30)
  end
  appointment(patient_id,date)
  on_retrovirals_state(patient_id,date,outcome)
end

def appointment(patient_id,date)
  puts "Now creating appointment encounter for patient: #{patient_id}"
  encounter_id = create_encounter(patient_id,7,date)
  next_date = date+30
  obs_value_datetime(patient_id,encounter_id,5096,date,next_date)
end

def on_retrovirals_state(patient_id,date,outcome)
  puts "Now creating on retrovirals outcome for patient: #{patient_id}"
  patient_prog_id = PatientProgram.where(patient_id: patient_id).first.id
  #Delete previous  states and recreate them
  pat_states = PatientState.where(patient_program_id: patient_prog_id).destroy_all
  start_date = date
  end_date = start_date+1
  state = 0
  case outcome
  when "TO"
    state = 2
    #create on ART state first
    patient_state(patient_prog_id,7,start_date,end_date)
    #Now #create outcome state
    patient_state(patient_prog_id,state,end_date)
  when "Def"
    #just create last visit state
    patient_state(patient_prog_id,7,start_date,end_date)
  when "D"
    state = 3
    patient_state(patient_prog_id,7,start_date,end_date)
    #Now #create outcome state
    patient_state(patient_prog_id,state,end_date)
  when "Stop"
    state = 6
    patient_state(patient_prog_id,7,start_date,end_date)
    #Now #create outcome state
    patient_state(patient_prog_id,state,end_date)
  end

end

#utility methods
def create_encounter(patient_id,encounter_type_id,date)
  encounter_id = Encounter.create(:patient_id=>patient_id,:provider_id=>1,
                                   :location_id=>764,:encounter_datetime=>date,
                                   :program_id=>1,:encounter_type=>encounter_type_id,
                                   :creator=>1).id
  return encounter_id
end

def obs_value_coded(patient_id,encounter_id,concept_id,date,value)
  obs_id = Observation.create(:person_id=>patient_id,:encounter_id=>encounter_id,
                               :concept_id=>concept_id,:obs_datetime=>date,
                               :location_id=>764,:value_coded=>value,
                               :comments=> "Migrated from eMastercard",
                               :creator=>1
                               ).id
  return obs_id
end

def obs_value_numeric(patient_id,encounter_id,concept_id,date,value)
  obs_id = Observation.create(:person_id=>patient_id,:encounter_id=>encounter_id,
                               :concept_id=>concept_id,:obs_datetime=>date,
                               :location_id=>764,:value_numeric=>value,
                               :comments=> "Migrated from eMastercard",
                               :creator=>1
                               ).id
  return obs_id
end

def obs_value_text(patient_id,encounter_id,concept_id,date,value)
  obs_id = Observation.create(:person_id=>patient_id,:encounter_id=>encounter_id,
                               :concept_id=>concept_id,:obs_datetime=>date,
                               :location_id=>764,:value_text=>value,
                               :comments=> "Migrated from eMastercard",
                               :creator=>1
                               ).id
  return obs_id
end

def obs_value_drug_numeric(patient_id,encounter_id,concept_id,date,value_drug,value)
  obs_id = Observation.create(:person_id=>patient_id,:encounter_id=>encounter_id,
                               :concept_id=>concept_id,:obs_datetime=>date,
                               :location_id=>764,:value_drug=>value_drug,:value_numeric=>value,
                               :comments=> "Migrated from eMastercard",
                               :creator=>1
                               ).id
  return obs_id
end

def obs_value_datetime(patient_id,encounter_id,concept_id,date,value)

  obs_id = Observation.create(:person_id=>patient_id,:encounter_id=>encounter_id,
                               :concept_id=>concept_id,:obs_datetime=>date,
                               :location_id=>764,:value_datetime=>value,
                               :comments=> "Migrated from eMastercard",
                               :creator=>1
                               ).id
  return obs_id

end

def patient_state(patient_prog_id,state,start_date,end_date=nil)

  pat_state = PatientState.create(:patient_program_id => patient_prog_id,
                                  :state => state,
                                  :start_date => start_date,
                                  :end_date => end_date,
                                  :creator => 1
                                 )

end

def patient_age(patient_id,visit_date)
  dob = Person.find(patient_id).birthdate
  age = (visit_date.to_date-dob).to_i/365 rescue 0
  return age
end

def self.patient_gender(patient_id)
  gender = Person.find(patient_id).gender
end

#Code for other checks

def create_tmp_patient_table
  ActiveRecord::Base.connection.execute('DROP TABLE IF EXISTS temp_earliest_start_date')
  ActiveRecord::Base.connection.execute(
    'CREATE TABLE IF NOT EXISTS temp_earliest_start_date (
       patient_id INTEGER PRIMARY KEY,
       date_enrolled DATE NOT NULL,
       earliest_start_date DATETIME,
       birthdate DATE DEFAULT NULL,
       birthdate_estimated BOOLEAN,
       death_date DATE,
       gender VARCHAR(32),
       age_at_initiation INT DEFAULT NULL,
       age_in_days INT DEFAULT NULL
    ) ENGINE=MEMORY;'
  )

  ActiveRecord::Base.connection.execute(
    'CREATE INDEX patient_id_index ON temp_earliest_start_date (patient_id)'
  )
  ActiveRecord::Base.connection.execute(
    'CREATE INDEX date_enrolled_index ON temp_earliest_start_date (date_enrolled)'
  )

  ActiveRecord::Base.connection.execute(
    'CREATE INDEX patient_id__date_enrolled_index ON temp_earliest_start_date (patient_id, date_enrolled)'
  )

  ActiveRecord::Base.connection.execute(
    'CREATE INDEX earliest_start_date_index ON temp_earliest_start_date (earliest_start_date)'
  )
  ActiveRecord::Base.connection.execute(
    'CREATE INDEX earliest_start_date__date_enrolled_index ON temp_earliest_start_date (earliest_start_date, date_enrolled)'
  )
end

STATE_DIED = 3
STATE_ON_TREATMENT = 7

def load_data_into_temp_earliest_start_date(end_date)
  ActiveRecord::Base.connection.execute <<EOF
  INSERT INTO temp_earliest_start_date
    select
      `p`.`patient_id` AS `patient_id`,
      cast(patient_date_enrolled(`p`.`patient_id`) as date) AS `date_enrolled`,
      date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`)) AS `earliest_start_date`,
      `pe`.`birthdate`,
      `pe`.`birthdate_estimated`,
      COALESCE(`died_outcome`.`start_date`, `pe`.`death_date`) AS `death_date`,
      `pe`.`gender` AS `gender`,
      (select timestampdiff(year, `pe`.`birthdate`, min(`s`.`start_date`))) AS `age_at_initiation`,
      (select timestampdiff(day, `pe`.`birthdate`, min(`s`.`start_date`))) AS `age_in_days`
    from
      ((`patient_program` `p`
      left join `person` `pe` ON ((`pe`.`person_id` = `p`.`patient_id`))
      left join `patient_state` `s` ON ((`p`.`patient_program_id` = `s`.`patient_program_id`)))
      left join `person` ON ((`person`.`person_id` = `p`.`patient_id`)))
      left join (SELECT patient_program_id, start_date FROM patient_state WHERE state = #{STATE_DIED} AND voided = 0)
        AS died_outcome ON p.patient_program_id = died_outcome.patient_program_id
    where
      ((`p`.`voided` = 0)
          and (`s`.`voided` = 0)
          and (`p`.`program_id` = 1)
          and (`s`.`state` = #{STATE_ON_TREATMENT}))
          and (DATE(`s`.`start_date`) >= '1900-01-1 00:00:00')
    group by `p`.`patient_id`
    HAVING date_enrolled IS NOT NULL;
EOF

  concept_id = ConceptName.find_by_name('Type of patient').concept_id
  ext_concept_id = ConceptName.find_by_name('External consultation').concept_id

  person_ids = Observation.where(concept_id: concept_id,
    value_coded: ext_concept_id).group(:person_id).map(&:person_id)

  unless person_ids.blank?
    ActiveRecord::Base.connection.execute <<EOF
    DELETE FROM temp_earliest_start_date WHERE patient_id IN(#{person_ids.join(',')});
EOF

  end

end

def update_cum_outcome(end_date)
  ActiveRecord::Base.connection.execute(
    'DROP TABLE IF EXISTS `temp_patient_outcomes`'
  )

  ActiveRecord::Base.connection.execute(
    "CREATE TABLE temp_patient_outcomes ENGINE=MEMORY AS (
      SELECT e.patient_id, patient_outcome(e.patient_id, '#{end_date} 23:59:59') AS cum_outcome
      FROM temp_earliest_start_date e WHERE e.date_enrolled <= '#{end_date}'
    )"
  )

  ActiveRecord::Base.connection.execute(
    'ALTER TABLE temp_patient_outcomes
     ADD INDEX patient_id_index (patient_id)'
  )

  ActiveRecord::Base.connection.execute(
    'ALTER TABLE temp_patient_outcomes
     ADD INDEX cum_outcome_index (cum_outcome)'
  )

  ActiveRecord::Base.connection.execute(
    'ALTER TABLE temp_patient_outcomes
     ADD INDEX patient_id_cum_outcome_index (patient_id, cum_outcome)'
  )
end

def get_patients_with_outcomes

     patients= ActiveRecord::Base.connection.select_all(
                     "SELECT * FROM temp_patient_outcomes WHERE cum_outcome <> 'Unknown' GROUP BY patient_id"
                  )
     return patients
end

def get_patient_identifiers(patient_ids)


  patients= ActiveRecord::Base.connection.select_all(
              "SELECT * FROM patient_identifier WHERE identifier_type = 4 AND patient_id
                NOT IN (#{patient_ids.join(',')}) GROUP BY patient_id"
                ).map{|identifier| identifier["identifier"].split("-").last.to_i}.sort!

end

def missing_patient_finder(site_code,end_date)
  create_tmp_patient_table
  load_data_into_temp_earliest_start_date(end_date)
  update_cum_outcome(end_date)
  patient_ids = get_patients_with_outcomes.map{|pat| pat["patient_id"]}
  patient_arv_ids = get_patient_identifiers(patient_ids)

     unless patient_arv_ids.blank?
      puts "After migration cohort report could be missing the following ARV numbers:"
      patient_arv_ids.each do |arv_id|
           puts "======== #{site_code.upcase}-ARV-#{arv_id} ====="
         end
     end
end

def get_patient_with_outcomes_inside_visit(missing_arv_ids)
       data = []
       missing_arv_ids.each do |id|
         line = get_patient_clinic_details_from_old_db(id)
        unless line.blank?
          data << line
        end
       end
    return data
end

def get_patient_clinic_details_from_old_db(identifier)
  adverse_outcome_concept = 48
  data = Hash.new()
  patient_id = get_patient_id_from_old_db(identifier)
  outcome = @connector.query(
                     "SELECT * FROM emastercard35.obs where person_id = #{patient_id} AND
                      concept_id = #{adverse_outcome_concept} AND value_text IN ('TO','D','Def')").first

   unless outcome.blank?
        data = {"ARV Number"=>identifier,"Outcome"=>outcome["value_text"],
                "Outcome Date"=>outcome["obs_datetime"].to_s}
   end
   return data
end

def get_patient_id_from_old_db(identifier)
  patient_id = @connector.query("SELECT patient_id FROM patient_identifier
                                                WHERE identifier= #{identifier}").first["patient_id"]
end

def get_regimen_from_old_db(arv_identifier)
  regimen_concept = 22
  patient_id = get_patient_id_from_old_db(arv_identifier)
  regimen = @connector.query("SELECT * FROM obs where person_id = #{patient_id} AND
                            concept_id = #{regimen_concept} AND value_text IS NOT null").first["value_text"] rescue nil
  return regimen
end

def old_emastercard_database
   config = YAML.load(File.open("../eMastercard2Nart/config.yaml"))
   @db_host  = config["emastercard"]["host"]
   @db_user  = config["emastercard"]["username"]
   @db_pass  = config["emastercard"]["password"]
   @db_name = config["emastercard"]["database"]

   connector = Mysql2::Client.new(:host => @db_host, :username => @db_user, :password => @db_pass, :database => @db_name)
  return connector
end


start




#current_outcome = ActiveRecord::Base.connection.select_one <<-SQL
      #SELECT patient_outcome(36, DATE('#{visit_date}')) outcome;
#SQL


