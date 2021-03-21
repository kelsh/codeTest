const fs = require('fs')
const parse = require('csv-parse/lib/sync')
const streamParse = require('csv-parse');

const fileSet = process.env.fileSet || 'large'

async function readFile(filepath){

    let data;
    try {
        data = fs.readFileSync(filepath, 'utf8')
    } catch (err) {
        console.error(err)
    }
    return data
}

async function syncParseCSV(filepath){

    let data = await readFile(filepath)
    
    if(!data){
        return []
    }

    const records = parse(data, {
        columns: true,
        skip_empty_lines: true,
        trim:true
    })

    return records
}

function findFacility(loan, {facilities,covenantsByFacility}){
    
    let bestFacility,bestCovenant,expectedYield;

    facilities.forEach( (facility)=>{

        if(bestFacility){ return }

        covenantsByFacility[facility.id].forEach((covenant)=>{
            if(
                parseFloat(covenant.max_default_likelihood) >= parseFloat(loan.default_likelihood)
                && covenant.banned_state !== loan.state
                && parseInt(loan.amount) <= parseInt(facility.amount)
            ){
                bestFacility = facility
                bestCovenant = covenant
                facility.amount = parseInt(facility.amount) - parseInt(loan.amount);
                expectedYield = ((1 - parseFloat(loan.default_likelihood)) * parseFloat(loan.interest_rate) * parseInt(loan.amount)) - (parseFloat(loan.default_likelihood) * parseInt(loan.amount)) - (parseFloat(facility.interest_rate) * parseInt(loan.amount))
                return;
            }
        })
    })

    if(bestFacility){
        return {
            assignment:{
                loan_id:loan.id,
                facility_id:bestFacility.id
            },
            yield:{
                facility_id:bestFacility.id,
                expected_yield:expectedYield
            }
        }
    }

    return {
        assignment:{
            loan_id:loan.id,
            facility_id:""
        }
    }
}

async function processLoans(filepath){

    const assignments =[]
    const yields = []

    const facilities = await syncParseCSV(`./${fileSet}/facilities.csv`).then((res)=>{
        return res.sort(function(a, b){return a.interest_rate-b.interest_rate});
    })
    
    const covenantsByFacility = await syncParseCSV(`./${fileSet}/covenants.csv`).then( (res)=>{
        
        return res.reduce( (acc, cur)=>{
            if(!acc[cur.facility_id]){
                acc[cur.facility_id]=[]
            }
    
            acc[cur.facility_id].push(cur)
            return acc
        },{})
    })

    const parser = streamParse({
        delimiter: ',',
        columns: true
    })

    parser.on('readable', function(){
        let loan,foundFacility;
        while (loan = parser.read()) {
            foundFacility = findFacility(loan,{facilities,covenantsByFacility})
            if(foundFacility.yield){
                yields.push(foundFacility.yield)
            }
            assignments.push(foundFacility.assignment)
        }
      })
    
      parser.on('error', function(err){
        console.error(err.message)
      })
     
      parser.on('end', function(){
        createYieldsFile(yields)
        createAssignmentsFile(assignments)
      })
      let data = await readFile(filepath)
      parser.write(data);
      parser.end()
}

function createAssignmentsFile(assignments){
    let headers = "loan_id,facility_id"+"\n"
    let body = assignments.map( assignment => assignment.loan_id+","+assignment.facility_id).join("\n")
    
    fs.writeFile('assignments.csv', headers+body, 'utf8', function (err) {
        if (err) {
            console.log('Some error occured - file either not saved or corrupted file saved.');
        } else{
            console.log('assignments.csv Created');
        }
    });
}

function createYieldsFile(yields){

    let addedYields = yields.reduce( (acc, curr) =>{
        if(!acc[curr.facility_id]){
            acc[curr.facility_id] = 0
        }
        
        acc[curr.facility_id] = acc[curr.facility_id] + curr.expected_yield

        return acc
    }, {});

    let headers = "facility_id,expected_yield"+"\n"
    let body = Object.keys(addedYields).map( (facilityId) => facilityId+","+Math.round(addedYields[facilityId]) ).join("\n")

    fs.writeFile('yields.csv', headers+body, 'utf8', function (err) {
        if (err) {
            console.log('Some error occured - file either not saved or corrupted file saved.');
        } else{
            console.log('yields.csv created');
        }
    });
}

function start(){

    processLoans(`./${fileSet}/loans.csv`);
}
start()
