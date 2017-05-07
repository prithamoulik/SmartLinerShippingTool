package main
import 
(
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"math"
	
	"github.com/hyperledger/fabric/core/chaincode/shim"
	//"github.com/hyperledger/fabric/core/crypto/primitives"
)

// SmartLinerShippingTool example simple Chaincode implementation
type SmartLinerShippingTool struct {
}

type ShipDetails struct{	
	ShipId string `json:"shipId"`
	VesselName string `json:"vesselName"`
	VoyageNo string `json:"voyageNo"`
	LinerCompanyName string `json:"linerCompanyName"`
	LinerCompanyId string `json:"linerCompanyId"`
	Latitude string `json:"latitude"`
	Longitude string `json:"longitude"`
	Capacity string `json:"capacity"`
	NumberOfContainerFilledUp string `json:"numberOfContainerFilledUp"`
}

type ContainerShippingDetails struct{	
	ShipId string `json:"shipId"`
	ContainerId string `json:"containerId"` //No. of container filled may be need to add
	
	
}
// ContainerDetails is for storing Container Details
type ContainerDetails struct{	
	ContainerId string `json:"containerId"`
	ShipId string `json:"shipId"`
	ContainerType string `json:"containerType"`
	Size string `json:"size"`
	Temperature string `json:"temperature"`
	CodeISO string `json:"codeISO"`
	
}
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}
func calcDistance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
  // must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378.1 // Earth radius in KM

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}

//find Nearest Ship
func (t *SmartLinerShippingTool) findNearestShip(stub shim.ChaincodeStubInterface, args []string) (string, error) {

	shipId := args[0]

	var lat1, long1, lat2, long2, distance float64
	distance = 20036 //Longest distance any 2 points on Earth
	var nearestShipId string
	nearestShipId = shipId
	
	if len(args) != 1 {
		return nearestShipId, errors.New("Incorrect number of arguments. Expecting ShipId to query")
	}
	
	var columns []shim.Column
	
	row,err := stub.GetRow("ShipDetails", []shim.Column{
		{Value: &shim.Column_String_{String_: shipId}},
	})
		
	lat1, _ = strconv.ParseFloat(row.Columns[5].GetString_(), 64)
	long1, _ = strconv.ParseFloat(row.Columns[6].GetString_()	, 64)
	
	rows, err := stub.GetRows("ShipDetails", columns)
	if err != nil {
		return nearestShipId, fmt.Errorf("Failed to retrieve row(s)")
	}
			
	for row := range rows {		
		newApp:= new(ShipDetails)
		newApp.ShipId = row.Columns[0].GetString_()
		newApp.VesselName = row.Columns[1].GetString_()
		newApp.VoyageNo = row.Columns[2].GetString_()
		newApp.LinerCompanyName = row.Columns[3].GetString_()
		newApp.LinerCompanyId = row.Columns[4].GetString_()
		newApp.Latitude = row.Columns[5].GetString_()
		newApp.Longitude = row.Columns[6].GetString_()
		newApp.Capacity = row.Columns[7].GetString_()
			
	if newApp.ShipId != shipId {
			lat2,_ = strconv.ParseFloat(newApp.Latitude, 64)
			long2,_ = strconv.ParseFloat(newApp.Longitude, 64)
			
			//Calculate the distance
			result := calcDistance(lat1, lat2, long1, long2)
			if result < distance {
				distance = result
				nearestShipId = newApp.ShipId
			}
					
		}
	}
	if distance < 0 {
		return nearestShipId, fmt.Errorf("No Ship found  Nearby")
	}
	
	return nearestShipId, nil
}

func main() {
	//primitives.SetSecurityLevel("SHA3", 256)
	err := shim.Start(new(SmartLinerShippingTool))
	if err != nil {
		fmt.Printf("Error starting SmartLinerShippingTool: %s", err)
	}
} 

// Init resets all the things
// Init initializes the smart contracts
func (t *SmartLinerShippingTool) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	// Check if table already exists
	_, err := stub.GetTable("ShipDetails")
	if err == nil {
		// Table already exists; do not recreate
		return nil, nil
	}

	// Create application Table
	err = stub.CreateTable("ShipDetails", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "shipId", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "vesselName", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "voyageNo", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "linerCompanyName", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "latitude", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "longitude", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "capacity", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "numberOfContainerFilledUp", Type: shim.ColumnDefinition_STRING, Key: false},
		
	})
	if err != nil {
		return nil, errors.New("Failed creating ShipDetails table.")
	}
	
	// Check if table already exists
	_, err = stub.GetTable("ContainerShippingDetails")
	if err == nil {
		// Table already exists; do not recreate
		return nil, nil
	}

	
	// Check if table already exists
	//CONTAINER TYPE - START
	_, err = stub.GetTable("ContainerDetails")
	if err == nil {
		// Table already exists; do not recreate
		return nil, nil
	}

	// Create ContainerDetails Table
	err = stub.CreateTable("ContainerDetails", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "containerId", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "shipId", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "containerType", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "size", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "temperature", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "codeISO", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {
		return nil, errors.New("Failed creating ContainerDetails table.")
	}
	//CONTAINER TYPE - END
	
	
	//CHECK Below - Can 2 states be given?
	stub.PutState("ContainerDetailsIncrement", []byte("1"))

	return nil, nil
}
// generate booking number for shipping item
func (t *SmartLinerShippingTool) addShipDetails(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

		if len(args) != 7 {
			return nil, fmt.Errorf("Incorrect number of arguments. Expecting 7. Got: %d.", len(args))
		}
		
		shipId:=args[0]
		vesselName:=args[1]
		voyageNo:=args[2]
		linerCompanyName:=args[3]
		latitude:=args[4]
		longitude:=args[5]
		capacity:=args[6]
		numberOfContainerFilledUp:=args[6]
		
		
		// Insert a row
		ok, err := stub.InsertRow("ShipDetails", shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: shipId}},
				&shim.Column{Value: &shim.Column_String_{String_: vesselName}},
				&shim.Column{Value: &shim.Column_String_{String_: voyageNo}},
				&shim.Column{Value: &shim.Column_String_{String_: linerCompanyName}},
				&shim.Column{Value: &shim.Column_String_{String_: latitude}},
				&shim.Column{Value: &shim.Column_String_{String_: longitude}},
				&shim.Column{Value: &shim.Column_String_{String_: capacity}},
				&shim.Column{Value: &shim.Column_String_{String_: numberOfContainerFilledUp}},
						
			}})

		if err != nil {
			return nil, err 
		}
		if !ok && err == nil {
			return nil, errors.New("Row already exists.")
		}
			
		return nil, nil

}	

//get all ship details for specified shipId
func (t *SmartLinerShippingTool) getShipDetailsByShipId(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting shipId to query")
	}

	shipId := args[0]
	
	var columns []shim.Column

	rows, err := stub.GetRows("ShipDetails", columns)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve row")
	}
			
	res2E:= []*ShipDetails{}	
	
	for row := range rows {		
		newApp:= new(ShipDetails)
		newApp.ShipId = row.Columns[0].GetString_()
		newApp.VesselName = row.Columns[1].GetString_()
		newApp.VoyageNo = row.Columns[2].GetString_()
		newApp.LinerCompanyName = row.Columns[3].GetString_()
		newApp.Latitude = row.Columns[4].GetString_()
		newApp.Longitude = row.Columns[5].GetString_()
		newApp.Capacity = row.Columns[6].GetString_()
		newApp.NumberOfContainerFilledUp = row.Columns[7].GetString_()
		
		
		if newApp.ShipId == shipId{
		res2E=append(res2E,newApp)		
		}				
	}
	
    mapB, _ := json.Marshal(res2E)
    fmt.Println(string(mapB))
	
	return mapB, nil

}

//view all ship details from ship_details table
func (t *SmartLinerShippingTool) viewAllShipDetails(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	var columns []shim.Column

	rows, err := stub.GetRows("ShipDetails", columns)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve row")
	}
			
	res2E:= []*ShipDetails{}	
	
	for row := range rows {		
		newApp:= new(ShipDetails)
		newApp.ShipId = row.Columns[0].GetString_()
		newApp.VesselName = row.Columns[1].GetString_()
		newApp.VoyageNo = row.Columns[2].GetString_()
		newApp.LinerCompanyName = row.Columns[3].GetString_()
		newApp.Latitude = row.Columns[4].GetString_()
		newApp.Longitude = row.Columns[5].GetString_()
		newApp.Capacity = row.Columns[6].GetString_()
		newApp.NumberOfContainerFilledUp = row.Columns[7].GetString_()
		
		res2E=append(res2E,newApp)		
					
	}
	
    mapB, _ := json.Marshal(res2E)
    fmt.Println(string(mapB))
	
	return mapB, nil

}


//Loading a Container in a Liner
func (t *SmartLinerShippingTool) loadContainerIntoLiner(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

		if len(args) != 6  {
			return nil, fmt.Errorf("Incorrect number of arguments. Expecting 6. Got: %d.", len(args))
		}
		
		Avalbytes, err := stub.GetState("ContainerDetailsIncrement") 
		Aval, _ := strconv.ParseInt(string(Avalbytes), 10, 0) 
		newAval:=int(Aval) + 1 
		newContainerDetailsIncrement:= strconv.Itoa(newAval) 
		stub.PutState("ContainerDetailsIncrement", []byte(newContainerDetailsIncrement))
		containerId:=string(Avalbytes)
		
		shipId:=args[0]
		containerType:=args[1]
		size:=args[2]
				
		temperature:=args[3]
		codeISO:=args[4]
				
		// Insert a row
		ok, err := stub.InsertRow("ContainerDetails", shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: containerId}},
				&shim.Column{Value: &shim.Column_String_{String_: shipId}},
				&shim.Column{Value: &shim.Column_String_{String_: containerType}},
				&shim.Column{Value: &shim.Column_String_{String_: size}},
				&shim.Column{Value: &shim.Column_String_{String_: temperature}},
				&shim.Column{Value: &shim.Column_String_{String_: codeISO}},
			}})

		if err != nil {
			return nil, err 
		}
		if !ok && err == nil {
			return nil, errors.New("Row already exists.")
		}
			
		return nil, nil

}	

//get all Container details for specified Id
func (t *SmartLinerShippingTool) getContainerDetailsByShipId(stub shim.ChaincodeStubInterface, args []string) string {

	if len(args) != 1 {
		fmt.Println("Incorrect number of arguments. Expecting shipid to query")
		return "0"
	}

	shipId := args[0]
	
	var columns []shim.Column

	rows, err := stub.GetRows("ContainerDetails", columns)
	if err != nil {
		fmt.Println("Failed to retrieve row")
		return "0"
	}
			
	
	
	for row := range rows {		
		newApp:= new(ContainerDetails)
		newApp.ContainerId = row.Columns[0].GetString_()
		newApp.ShipId = row.Columns[1].GetString_()
		newApp.ContainerType = row.Columns[2].GetString_()
		newApp.Size = row.Columns[3].GetString_()
		newApp.Temperature = row.Columns[4].GetString_()
		newApp.CodeISO = row.Columns[5].GetString_()
				
		if newApp.ShipId == shipId {			
            return newApp.ContainerId			
		}				
	}
	return "0"
    
}

//update ship location by ship id
func (t *SmartLinerShippingTool) updateShipLocation(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 3.")
	}
	shipId := args[0]
	newLatitude := args[1]	
	newLongitude := args[2]
	
	var columns []shim.Column

	rows, err := stub.GetRows("ShipDetails", columns)
	if err != nil {
			return nil, fmt.Errorf("Failed to retrieve row")
	}

	for row := range rows {	
		
		tempShipId := row.Columns[0].GetString_()
		if tempShipId==shipId{
			
			
			// Get the row pertaining to this bookingNo
			var columns1 []shim.Column
			col1 := shim.Column{Value: &shim.Column_String_{String_: tempShipId}}
			columns1 = append(columns1, col1)

			row, err := stub.GetRow("ShipDetails", columns1)
			if err != nil {
				return nil, fmt.Errorf("Error: Failed retrieving data with shipId %s. Error %s", tempShipId, err.Error())
			}

			// GetRows returns empty message if key does not exist
			if len(row.Columns) == 0 {
				return nil, nil
			}
			
			//End- Check that the currentStatus to newStatus transition is accurate
			// Delete the row pertaining to this applicationId
			err = stub.DeleteRow(
				"ShipDetails",
				columns1,
			)
			if err != nil {
				return nil, errors.New("Failed deleting row while updating ship location.")
			}
			shipId := row.Columns[0].GetString_()
			vesselName := row.Columns[1].GetString_()
			voyageNo := row.Columns[2].GetString_()
			linerCompanyName := row.Columns[3].GetString_()
			latitude := newLatitude
			longitude := newLongitude
			capacity := row.Columns[6].GetString_()
			
			
			// Insert a row
			ok, err := stub.InsertRow("ShipDetails", shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: shipId}},
					&shim.Column{Value: &shim.Column_String_{String_: vesselName}},
					&shim.Column{Value: &shim.Column_String_{String_: voyageNo}},
					&shim.Column{Value: &shim.Column_String_{String_: linerCompanyName}},
					&shim.Column{Value: &shim.Column_String_{String_: latitude}},
					&shim.Column{Value: &shim.Column_String_{String_: longitude}},
					&shim.Column{Value: &shim.Column_String_{String_: capacity}},
						
				}})

			if err != nil {
				return nil, err 
			}
			if !ok && err == nil {
				return nil, errors.New("Failed to insert row while updating location [latitude, longitude] status.")
			}
		}
	}
		return nil, nil

}
//update container's shipid  
func (t *SmartLinerShippingTool) moveContainerToDifferentLinerShip(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2..containerId and destinationShipId.")
	}
	containerId := args[0]
	destinationShipId := args[1]	
	
	
	var columns []shim.Column

	rows, err := stub.GetRows("ContainerDetails", columns)
	if err != nil {
			return nil, fmt.Errorf("Failed to retrieve row")
	}

	for row := range rows {	
		
		tempContainerId := row.Columns[0].GetString_()
		if tempContainerId==containerId{		
			
			
			var columns1 []shim.Column
			col1 := shim.Column{Value: &shim.Column_String_{String_: tempContainerId}}
			columns1 = append(columns1, col1)

			row, err := stub.GetRow("ContainerDetails", columns1)
			if err != nil {
				return nil, fmt.Errorf("Error: Failed retrieving data with shipId %s. Error %s", tempContainerId, err.Error())
			}

			// GetRows returns empty message if key does not exist
			if len(row.Columns) == 0 {
				return nil, nil
			}
			
			//End- Check that the currentStatus to newStatus transition is accurate
			// Delete the row pertaining to this applicationId
			err = stub.DeleteRow(
				"ContainerDetails",
				columns1,
			)
			if err != nil {
				return nil, errors.New("Failed deleting row while updating ContainerDetails shipid.")
			}
			containerId := row.Columns[0].GetString_()
			shipId := destinationShipId
			containerType := row.Columns[2].GetString_()
			size := row.Columns[3].GetString_()
			temperature := row.Columns[4].GetString_()
			codeISO := row.Columns[5].GetString_()
			
			
			// Insert a row
			ok, err := stub.InsertRow("ContainerDetails", shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: containerId}},
					&shim.Column{Value: &shim.Column_String_{String_: shipId}},
					&shim.Column{Value: &shim.Column_String_{String_: containerType}},
					&shim.Column{Value: &shim.Column_String_{String_: size}},
					&shim.Column{Value: &shim.Column_String_{String_: temperature}},
					&shim.Column{Value: &shim.Column_String_{String_: codeISO}},
						
				}})

			if err != nil {
				return nil, err 
			}
			if !ok && err == nil {
				return nil, errors.New("Failed to insert row while updating new shipid.")
			}
		}
	}
		return nil, nil

}

//find out free space in the ship
func (t *SmartLinerShippingTool) searchForFreeSpace(stub shim.ChaincodeStubInterface, args []string) bool {
	if len(args) != 1 {
	    fmt.Println("Need to pass one argument that is shipid: ")
		return false
	}

	shipId := args[0]
	
	var columns []shim.Column

	rows, err := stub.GetRows("ShipDetails", columns)
	if err != nil {
		return false
	}
			
	
	var isFreeSpaceAvailable bool
	isFreeSpaceAvailable = true
	
	for row := range rows {		
		newApp:= new(ShipDetails)
		newApp.ShipId = row.Columns[0].GetString_()
		newApp.VesselName = row.Columns[1].GetString_()
		newApp.VoyageNo = row.Columns[2].GetString_()
		newApp.LinerCompanyName = row.Columns[3].GetString_()
		newApp.Latitude = row.Columns[4].GetString_()
		newApp.Longitude = row.Columns[5].GetString_()
		newApp.Capacity = row.Columns[6].GetString_()
		newApp.NumberOfContainerFilledUp = row.Columns[7].GetString_()
		
		if newApp.ShipId == shipId{		
			if newApp.NumberOfContainerFilledUp == newApp.Capacity {
			 isFreeSpaceAvailable = false
			}
		}
		
		
			
	}
    return isFreeSpaceAvailable
}

//Raise event to move container
func (t *SmartLinerShippingTool) raiseEventToMoveContainer(stub shim.ChaincodeStubInterface, args []string)  ([]byte, error) {
	if len(args) != 1 {
	    fmt.Println("Need to pass one argument that is shipid: ")
		return nil, nil
	}
	var sourceShipId, destinationShipId, params [] string
	sourceShipId[0] = args[0]
	//t := SmartLinerShippingTool{}
	destinationShipId[0], _ = t.findNearestShip (stub, sourceShipId)
	containerIdToMove := t.getContainerDetailsByShipId(stub, sourceShipId)
	isFreeSpaceAvailableInDestinationShip := t.searchForFreeSpace(stub, destinationShipId)
	if isFreeSpaceAvailableInDestinationShip == true {
	     params[0] = containerIdToMove
		 params[1] = destinationShipId[0]
	     t.moveContainerToDifferentLinerShip(stub, params )
	}
	return nil, nil
}


// Invoke is the entry point to execute Insert/Update/Delete type chaincode function
func (t *SmartLinerShippingTool) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	fmt.Println("invoke is running " + function)

	// Handle different functions
	if function == "addShipDetails" {
		t := SmartLinerShippingTool{}
		return t.addShipDetails(stub, args)	
	} else if function == "loadContainerIntoLiner" {
		return t.loadContainerIntoLiner(stub, args)
	}else if function == "updateShipLocation" {
		return t.updateShipLocation(stub, args)
	}else if function == "raiseEventToMoveContainer" {
		return t.raiseEventToMoveContainer(stub, args)
	}
	fmt.Println("invoke did not find func: " + function)

	return nil, errors.New("Received unknown function invocation: " + function)
}
// Query is our entry point for queries
func (t *SmartLinerShippingTool) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

    if function == "getShipDetailsByShipId" {
		return t.getShipDetailsByShipId(stub, args)
	}else if function == "viewAllShipDetails" {
		return t.viewAllShipDetails(stub, args)
	}
		
	return nil, errors.New("Invalid query function name.")
}

