require "csv"
def start
  path = "./bin/kaphatenga_unknown_reason.csv"
  patients = CSV.parse(File.read(path), headers: true)
  process(patients)
end

def process(patients)
    patients.each do |patient|
      #puts "Processing ARV No: #{patient["ARV number"].upcase}"
      patient_id = PatientIdentifier.where(identifier: patient["ARV number"].upcase).first.patient_id
      reason = patient["Reason"]
      #initialize eligibility to unknown
      eligibility = Concept.find_by_name('Unknown').concept_id
      puts "Updating patient with ID: #{patient_id}"

      case reason
      when "Stage 4"
        eligibility = Concept.find_by_name('WHO STAGE 4').concept_id
      when "Stage 3"
        eligibility = Concept.find_by_name('WHO STAGE 3').concept_id
      when "CD4"
        eligibility = Concept.find_by_name('CD4 COUNT LESS THAN OR EQUAL TO 250').concept_id
      when "BF"
        eligibility = Concept.find_by_name('Breastfeeding').concept_id
      when "Preg"
        eligibility = Concept.find_by_name('PATIENT PREGNANT').concept_id
      when "U5"
        eligibility = Concept.find_by_name('HIV Infected').concept_id
      when "PCR"
        eligibility = Concept.find_by_name('HIV PCR').concept_id
      when "PSHD"
        eligibility = Concept.find_by_name('PRESUMED SEVERE HIV CRITERIA IN INFANTS').concept_id
      when "ASYMP"
        eligibility = Concept.find_by_name('ASYMPTOMATIC').concept_id
      end
      #check staging encounter

      encounter = Encounter.where({patient_id: patient_id, encounter_type: 52}).first.id rescue nil
      if encounter.blank?
        #create anounter one
        encounter = create_encounter(patient_id,52,Date.today)
      end
      eligibility_obs = Observation.where({concept_id: 7563,person_id: patient_id,voided: 0})
      if eligibility_obs.present?
        eligibility_obs.update({value_coded: eligibility,voided:0,encounter_id: encounter})
      else
        #create the observation
        obs_value_coded(patient_id,encounter,7563,Date.today,eligibility)
      end
    end
end

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

start


