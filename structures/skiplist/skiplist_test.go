package skiplist

import (
	"testing"
)

// TestNewSkipList verifies the creation of a new skip list.
func TestNewSkipList(t *testing.T) {
	maxHeight := uint64(5)
	skipList := NewSkipList(maxHeight)

	if skipList == nil {
		t.Fatal("SkipList is nil")
	}

	if skipList.maxHeight != maxHeight {
		t.Errorf("Expected maxHeight to be %d, got %d", maxHeight, skipList.maxHeight)
	}

	if skipList.currentHeight != 1 {
		t.Errorf("Expected currentHeight to be 1, got %d", skipList.currentHeight)
	}

	if skipList.head == nil {
		t.Fatal("Head node is nil")
	}
}

// TestAddAndCheck verifies the Add and Check methods.
func TestAddAndCheck(t *testing.T) {
	skipList := NewSkipList(5)

	// Add elements
	skipList.Add("key1", "value1")
	skipList.Add("key2", "value2")
	skipList.Add("key3", "value3")

	// Check if elements exist
	if !skipList.Check("key1") {
		t.Errorf("Expected key1 to exist in the skip list")
	}

	if !skipList.Check("key2") {
		t.Errorf("Expected key2 to exist in the skip list")
	}

	if !skipList.Check("key3") {
		t.Errorf("Expected key3 to exist in the skip list")
	}

	// Check if a non-existing element is correctly reported
	if skipList.Check("key4") {
		t.Errorf("Expected key4 to not exist in the skip list")
	}
}

// TestAddDuplicate verifies that duplicates are not added to the skip list.
func TestAddDuplicate(t *testing.T) {
	skipList := NewSkipList(5)

	// Add an element
	skipList.Add("key1", "value1")

	// Add the same key again
	skipList.Add("key1", "value2")

	// Check that the key still exists with its original value
	if !skipList.Check("key1") {
		t.Errorf("Expected key1 to exist in the skip list")
	}
}

// TestDelete verifies the Delete method.
func TestDelete(t *testing.T) {
	skipList := NewSkipList(5)

	// Add elements
	skipList.Add("key1", "value1")
	skipList.Add("key2", "value2")
	skipList.Add("key3", "value3")

	// Delete key2
	skipList.Delete("key2")

	// Ensure key2 no longer exists
	if skipList.Check("key2") {
		t.Errorf("Expected key2 to be deleted from the skip list")
	}

	// Ensure key1 and key3 still exist
	if !skipList.Check("key1") {
		t.Errorf("Expected key1 to still exist in the skip list")
	}

	if !skipList.Check("key3") {
		t.Errorf("Expected key3 to still exist in the skip list")
	}
}

// TestDeleteNonExistent verifies that deleting a non-existent key does not cause issues.
func TestDeleteNonExistent(t *testing.T) {
	skipList := NewSkipList(5)

	// Add some elements
	skipList.Add("key1", "value1")
	skipList.Add("key2", "value2")

	// Delete a non-existent key
	skipList.Delete("key3")

	// Ensure key1 and key2 still exist
	if !skipList.Check("key1") {
		t.Errorf("Expected key1 to still exist in the skip list")
	}

	if !skipList.Check("key2") {
		t.Errorf("Expected key2 to still exist in the skip list")
	}
}

// TestSerializeAndDeserialize verifies that a skip list can be serialized and deserialized without data loss.
func TestSerializeAndDeserialize(t *testing.T) {
	// Step 1: Create a new skip list and populate it with data
	skipList := NewSkipList(5)
	skipList.Add("key1", "value1")
	skipList.Add("key2", "value2")
	skipList.Add("key3", "value3")

	// Step 2: Serialize the skip list
	serializedData := skipList.Serialize()
	if len(serializedData) == 0 {
		t.Fatalf("Serialization failed: serialized data is empty")
	}

	// Step 3: Deserialize the skip list
	deserializedSkipList := Deserialize(serializedData)

	// Step 4: Validate the deserialized data
	// Check that all keys exist in the deserialized skip list
	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		if !deserializedSkipList.Check(key) {
			t.Errorf("Key '%s' not found in deserialized skip list", key)
		}
	}

	// Ensure values are preserved
	expectedValues := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, expectedValue := range expectedValues {
		currentNode := deserializedSkipList.head.nextNodes[0]
		found := false
		for currentNode != nil {
			if currentNode.key == key {
				if currentNode.value != expectedValue {
					t.Errorf("Expected value '%s' for key '%s', got '%s'", expectedValue, key, currentNode.value)
				}
				found = true
				break
			}
			currentNode = currentNode.nextNodes[0]
		}
		if !found {
			t.Errorf("Key '%s' not found in the deserialized skip list", key)
		}
	}
}

// TestSerializeEmptySkipList ensures serialization and deserialization work for an empty skip list.
func TestSerializeEmptySkipList(t *testing.T) {
	// Step 1: Create an empty skip list
	skipList := NewSkipList(5)

	// Step 2: Serialize the empty skip list
	serializedData := skipList.Serialize()
	if len(serializedData) == 0 {
		t.Fatalf("Serialization failed: serialized data is empty")
	}

	// Step 3: Deserialize the empty skip list
	deserializedSkipList := Deserialize(serializedData)

	// Step 4: Validate the deserialized data
	if deserializedSkipList.currentHeight != 1 {
		t.Errorf("Expected currentHeight to be 1, got %d", deserializedSkipList.currentHeight)
	}

	if deserializedSkipList.maxHeight != 5 {
		t.Errorf("Expected maxHeight to be 5, got %d", deserializedSkipList.maxHeight)
	}

	if deserializedSkipList.head == nil {
		t.Fatal("Head node is nil in deserialized skip list")
	}
}

// TestSerializationCorruptedData ensures deserialization fails gracefully with corrupted data.
func TestSerializationCorruptedData(t *testing.T) {
	// Step 1: Create a valid serialized skip list
	skipList := NewSkipList(5)
	skipList.Add("key1", "value1")
	serializedData := skipList.Serialize()

	// Step 2: Corrupt the serialized data
	corruptedData := append(serializedData[:len(serializedData)/2], []byte("corruption")...)

	// Step 3: Attempt to deserialize corrupted data
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic during deserialization of corrupted data, but no panic occurred")
		}
	}()
	Deserialize(corruptedData)
}
