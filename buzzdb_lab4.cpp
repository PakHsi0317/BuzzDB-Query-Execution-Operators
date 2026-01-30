#include <iostream>
#include <map>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <chrono>
#include <list>
#include <unordered_map>
#include <iostream>
#include <map>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <thread>
#include <queue>
#include <optional>
#include <regex>
#include <stdexcept>
#include <cassert>
#include <random>
#include <tuple>
#include <unordered_set>
#include <set>
#include <cstring>
#include <algorithm>


using namespace std::literals::string_literals;

#define UNUSED(p)  ((void)(p))

#define ASSERT_WITH_MESSAGE(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion \033[1;31mFAILED\033[0m: " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::abort(); \
        } \
    } while(0)


enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    size_t data_length;
    std::unique_ptr<char[]> data;

public:
    Field(): type(FieldType::STRING), data_length(0), data(nullptr) {}

    Field(int i) : type(FieldType::INT) { 
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FieldType::FLOAT) { 
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(FieldType::STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        
        // Reallocate if necessary
        if (data_length > 0 && other.data) {
            data = std::make_unique<char[]>(data_length);
            std::memcpy(data.get(), other.data.get(), data_length);
        } else {
            data.reset();
        }
        return *this;
    }

   // Copy constructor
    Field(const Field& other) : type(other.type), data_length(other.data_length) {
        if (data_length > 0 && other.data) {
            data = std::make_unique<char[]>(data_length);
            std::memcpy(data.get(), other.data.get(), data_length);
        }
    }

    // Move constructor - If you already have one, ensure it's correctly implemented
    Field(Field&& other) noexcept : type(other.type), data_length(other.data_length), data(std::move(other.data)) {
        // Optionally reset other's state if needed
    }

    FieldType getType() const {
        return type;
    }
    
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string asString() const { 
        switch (type) {
            case FieldType::INT:
                return std::to_string(asInt());
            case FieldType::FLOAT:
                return std::to_string(asFloat());
            case FieldType::STRING:
                return std::string(data.get());
            default:
                return "";
        }
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == FieldType::STRING) {
            buffer << data.get() << ' ';
        } else if (type == FieldType::INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FieldType::FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type; in >> type;
        size_t length; in >> length;
        
        // Skip the space after length
        in.get();
        
        if (type == FieldType::STRING) {
            // Read exactly 'length' characters (including null terminator)
            std::vector<char> buffer(length);
            in.read(buffer.data(), length - 1);  // Read length-1 chars (excluding null terminator)
            buffer[length - 1] = '\0';
            std::string val(buffer.data());
            
            // Skip the trailing space
            in.get();
            
            return std::make_unique<Field>(val);
        } else if (type == FieldType::INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FieldType::FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    // Clone method
    std::unique_ptr<Field> clone() const {
        // Use the copy constructor
        return std::make_unique<Field>(*this);
    }

    void print() const{
        switch(getType()){
            case FieldType::INT: std::cout << asInt(); break;
            case FieldType::FLOAT: std::cout << asFloat(); break;
            case FieldType::STRING: std::cout << asString(); break;
        }
    }
};


bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.type != rhs.type) return false; // Different types are never equal

    switch (lhs.type) {
        case FieldType::INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) == *reinterpret_cast<const int*>(rhs.data.get());
        case FieldType::FLOAT:
            return *reinterpret_cast<const float*>(lhs.data.get()) == *reinterpret_cast<const float*>(rhs.data.get());
        case FieldType::STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) == std::string(rhs.data.get(), rhs.data_length - 1);
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}


class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    // Clone method
    std::unique_ptr<Tuple> clone() const {
        auto clonedTuple = std::make_unique<Tuple>();
        for (const auto& field : fields) {
            clonedTuple->addField(field->clone());
        }
        return clonedTuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;  // Fixed page size
static constexpr size_t MAX_SLOTS = 512;   // Fixed number of slots
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
    bool empty = true;                 // Is the slot empty?    
    uint16_t offset = INVALID_VALUE;    // Offset of the slot within the page
    uint16_t length = INVALID_VALUE;    // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage(){
        // Empty page -> initialize slot array inside page
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    // Add a tuple, returns true if it fits, false otherwise.
    bool addTuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        //std::cout << "Tuple size: " << tuple_size << " bytes\n";
        // assert(tuple_size == 38);

        // Check for first slot with enough space
        size_t slot_itr = 0;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == true and 
                slot_array[slot_itr].length >= tuple_size) {
                break;
            }
        }
        if (slot_itr == MAX_SLOTS){
            //std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
            return false;
        }

        // Identify the offset where the tuple will be placed in the page
        // Update slot meta-data if needed
        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;
        if (slot_array[slot_itr].offset == INVALID_VALUE){
            if(slot_itr != 0){
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            }
            else{
                offset = metadata_size;
            }

            slot_array[slot_itr].offset = offset;
        }
        else{
            offset = slot_array[slot_itr].offset;
        }

        if(offset + tuple_size >= PAGE_SIZE){
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        if (slot_array[slot_itr].length == INVALID_VALUE){
            slot_array[slot_itr].length = tuple_size;
        }

        // Copy serialized data into the page
        std::memcpy(page_data.get() + offset, 
                    serializedTuple.c_str(), 
                    tuple_size);

        return true;
    }

    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        size_t slot_itr = 0;
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if(slot_itr == index and
               slot_array[slot_itr].empty == false){
                slot_array[slot_itr].empty = true;
                break;
               }
        }

        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void print() const{
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false){
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
public:    
    std::fstream fileStream;
    size_t num_pages = 0;

public:
    StorageManager(){
        fileStream.open(database_filename, std::ios::in | std::ios::out | std::ios::trunc );
        if (!fileStream) {
            // If file does not exist, create it
            fileStream.clear(); // Reset the state
            fileStream.open(database_filename, std::ios::out | std::ios::trunc);
        }
        fileStream.close(); 
        fileStream.open(database_filename, std::ios::in | std::ios::out); 

        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;

        // std::cout << "Storage Manager :: Num pages: " << num_pages << "\n";        
        if(num_pages == 0){
            extend();
        }

    }

    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    // Read a page from disk
    std::unique_ptr<SlottedPage> load(uint16_t page_id) {
        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        // Read the content of the file into the page
        if(fileStream.read(page->page_data.get(), PAGE_SIZE)){
            //std::cout << "Page read successfully from file." << std::endl;
        }
        else{
            std::cerr << "Error: Unable to read data from the file. \n";
            exit(-1);
        }
        return page;
    }

    // Write a page to disk
    void flush(uint16_t page_id, const std::unique_ptr<SlottedPage>& page) {
        size_t page_offset = page_id * PAGE_SIZE;        

        // Move the write pointer
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page->page_data.get(), PAGE_SIZE);        
        fileStream.flush();
    }

    // Extend database file by one page
    void extend() {
        // std::cout << "Extending database file \n";

        // Create a slotted page
        auto empty_slotted_page = std::make_unique<SlottedPage>();

        // Move the write pointer
        fileStream.seekp(0, std::ios::end);

        // Write the page to the file, extending it
        fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
        fileStream.flush();

        // Update number of pages
        num_pages += 1;
    }

};

using PageID = uint16_t;

class Policy {
public:
    virtual bool touch(PageID page_id) = 0;
    virtual PageID evict() = 0;
    virtual ~Policy() = default;
};

void printList(std::string list_name, const std::list<PageID>& myList) {
        std::cout << list_name << " :: ";
        for (const PageID& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

class LruPolicy : public Policy {
private:
    // List to keep track of the order of use
    std::list<PageID> lruList;

    // Map to find a page's iterator in the list efficiently
    std::unordered_map<PageID, std::list<PageID>::iterator> map;

    size_t cacheSize;

public:

    LruPolicy(size_t cacheSize) : cacheSize(cacheSize) {}

    bool touch(PageID page_id) override {
        //printList("LRU", lruList);

        bool found = false;
        // If page already in the list, remove it
        if (map.find(page_id) != map.end()) {
            found = true;
            lruList.erase(map[page_id]);
            map.erase(page_id);            
        }

        // If cache is full, evict
        if(lruList.size() == cacheSize){
            evict();
        }

        if(lruList.size() < cacheSize){
            // Add the page to the front of the list
            lruList.emplace_front(page_id);
            map[page_id] = lruList.begin();
        }

        return found;
    }

    PageID evict() override {
        // Evict the least recently used page
        PageID evictedPageId = INVALID_VALUE;
        if(lruList.size() != 0){
            evictedPageId = lruList.back();
            map.erase(evictedPageId);
            lruList.pop_back();
        }
        return evictedPageId;
    }

};

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferManager {
private:
    using PageMap = std::unordered_map<PageID, std::unique_ptr<SlottedPage>>;

    StorageManager storage_manager;
    PageMap pageMap;
    std::unique_ptr<Policy> policy;

public:
    BufferManager(): 
    policy(std::make_unique<LruPolicy>(MAX_PAGES_IN_MEMORY)) {}

    std::unique_ptr<SlottedPage>& getPage(int page_id) {
        auto it = pageMap.find(page_id);
        if (it != pageMap.end()) {
            policy->touch(page_id);
            return pageMap.find(page_id)->second;
        }

        if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
            auto evictedPageId = policy->evict();
            if(evictedPageId != INVALID_VALUE){
                // std::cout << "Evicting page " << evictedPageId << "\n";
                storage_manager.flush(evictedPageId, 
                                      pageMap[evictedPageId]);
            }
        }

        auto page = storage_manager.load(page_id);
        policy->touch(page_id);
        // std::cout << "Loading page: " << page_id << "\n";
        pageMap[page_id] = std::move(page);
        return pageMap[page_id];
    }

    void flushPage(int page_id) {
        //std::cout << "Flush page " << page_id << "\n";
        storage_manager.flush(page_id, pageMap[page_id]);
    }

    void extend(){
        storage_manager.extend();
    }
    
    size_t getNumPages(){
        return storage_manager.num_pages;
    }

};

class HashIndex {
private:
    struct HashEntry {
        int key;
        int value;
        int position; // Final position within the array
        bool exists; // Flag to check if entry exists

        // Default constructor
        HashEntry() : key(0), value(0), position(-1), exists(false) {}

        // Constructor for initializing with key, value, and exists flag
        HashEntry(int k, int v, int pos) : key(k), value(v), position(pos), exists(true) {}    
    };

    static const size_t capacity = 100; // Hard-coded capacity
    HashEntry hashTable[capacity]; // Static-sized array

    size_t hashFunction(int key) const {
        return key % capacity; // Simple modulo hash function
    }

public:
    HashIndex() {
        // Initialize all entries as non-existing by default
        for (size_t i = 0; i < capacity; ++i) {
            hashTable[i] = HashEntry();
        }
    }

    void insertOrUpdate(int key, int value) {
        size_t index = hashFunction(key);
        size_t originalIndex = index;
        bool inserted = false;
        int i = 0; // Attempt counter

        do {
            if (!hashTable[index].exists) {
                hashTable[index] = HashEntry(key, value, true);
                hashTable[index].position = index;
                inserted = true;
                break;
            } else if (hashTable[index].key == key) {
                hashTable[index].value += value;
                hashTable[index].position = index;
                inserted = true;
                break;
            }
            i++;
            index = (originalIndex + i*i) % capacity; // Quadratic probing
        } while (index != originalIndex && !inserted);

        if (!inserted) {
            std::cerr << "HashTable is full or cannot insert key: " << key << std::endl;
        }
    }

   int getValue(int key) const {
        size_t index = hashFunction(key);
        size_t originalIndex = index;

        do {
            if (hashTable[index].exists && hashTable[index].key == key) {
                return hashTable[index].value;
            }
            if (!hashTable[index].exists) {
                break; // Stop if we find a slot that has never been used
            }
            index = (index + 1) % capacity;
        } while (index != originalIndex);

        return -1; // Key not found
    }

    // This method is not efficient for range queries 
    // as this is an unordered index
    // but is included for comparison
    std::vector<int> rangeQuery(int lowerBound, int upperBound) const {
        std::vector<int> values;
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists && hashTable[i].key >= lowerBound && hashTable[i].key <= upperBound) {
                std::cout << "Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
                values.push_back(hashTable[i].value);
            }
        }
        return values;
    }

    void print() const {
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists) {
                std::cout << "Position: " << hashTable[i].position << 
                ", Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
            }
        }
    }
};

class Operator {
    public:
    virtual ~Operator() = default;

    /// Initializes the operator.
    virtual void open() = 0;

    /// Tries to generate the next tuple. Return true when a new tuple is
    /// available.
    virtual bool next() = 0;

    /// Destroys the operator.
    virtual void close() = 0;

    /// This returns the pointers to the Fields of the generated tuple. When
    /// `next()` returns true, the Fields will contain the values for the
    /// next tuple. Each `Field` pointer in the vector stands for one attribute of the tuple.
    virtual std::vector<std::unique_ptr<Field>> getOutput() = 0;
};

class UnaryOperator : public Operator {
    protected:
    Operator* input;

    public:
    explicit UnaryOperator(Operator& input) : input(&input) {}

    ~UnaryOperator() override = default;
};

class BinaryOperator : public Operator {
    protected:
    Operator* input_left;
    Operator* input_right;

    public:
    explicit BinaryOperator(Operator& input_left, Operator& input_right)
        : input_left(&input_left), input_right(&input_right) {}

    // ~BinaryOperator() override = default;
};

struct FieldVectorHasher {
    std::size_t operator()(const std::vector<Field>& fields) const {
        std::size_t hash = 0;
        for (const auto& field : fields) {
            std::hash<std::string> hasher;
            std::size_t fieldHash = 0;

            // Depending on the type, hash the corresponding data
            switch (field.type) {
                case FieldType::INT: {
                    // Convert integer data to string and hash
                    int value = *reinterpret_cast<const int*>(field.data.get());
                    fieldHash = hasher(std::to_string(value));
                    break;
                }
                case FieldType::FLOAT: {
                    // Convert float data to string and hash
                    float value = *reinterpret_cast<const float*>(field.data.get());
                    fieldHash = hasher(std::to_string(value));
                    break;
                }
                case FieldType::STRING: {
                    // Directly hash the string data
                    std::string value(field.data.get(), field.data_length - 1); // Exclude null-terminator
                    fieldHash = hasher(value);
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported field type for hashing.");
            }

            // Combine the hash of the current field with the hash so far
            hash ^= fieldHash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        }
        return hash;
    }
};

struct FieldHasher {
    std::size_t operator()(const Field& field) const {
        std::hash<std::string> hasher;
        std::size_t hash = 0;

        // Depending on the type, hash the corresponding data
        switch (field.type) {
            case FieldType::INT: {
                // Convert integer data to string and hash
                int value = *reinterpret_cast<const int*>(field.data.get());
                hash = hasher(std::to_string(value));
                break;
            }
            case FieldType::FLOAT: {
                // Convert float data to string and hash
                float value = *reinterpret_cast<const float*>(field.data.get());
                hash = hasher(std::to_string(value));
                break;
            }
            case FieldType::STRING: {
                // Directly hash the string data
                std::string value(field.data.get(), field.data_length - 1); // Exclude null-terminator
                hash = hasher(value);
                break;
            }
            default:
                throw std::runtime_error("Unsupported field type for hashing.");
        }

        return hash;
    }
};


class ScanOperator : public Operator {
private:
    BufferManager& bufferManager;
    size_t currentPageIndex = 0;
    size_t currentSlotIndex = 0;
    std::unique_ptr<Tuple> currentTuple;
    std::string scan_relation = "";

public:
    ScanOperator(BufferManager& manager) : bufferManager(manager) {}

    ScanOperator(BufferManager& manager, std::string relation) : bufferManager(manager), scan_relation(relation) {}

    void open() override {
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset(); // Ensure currentTuple is reset
    }

    bool next() override {
        loadNextTuple();
        return currentTuple != nullptr;
    }

    void close() override {
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (currentTuple) {
            if(scan_relation.length() != 0) {
                // remove the last field i.e. relation name
                currentTuple->fields.pop_back();
            }
            return std::move(currentTuple->fields);
        }
        return {}; // Return an empty vector if no tuple is available
    }

private:
    void loadNextTuple() {
        while (currentPageIndex < bufferManager.getNumPages()) {
            auto& currentPage = bufferManager.getPage(currentPageIndex);
            if (!currentPage || currentSlotIndex >= MAX_SLOTS) {
                currentSlotIndex = 0; // Reset slot index when moving to a new page
            }

            char* page_buffer = currentPage->page_data.get();
            Slot* slot_array = reinterpret_cast<Slot*>(page_buffer);

            while (currentSlotIndex < MAX_SLOTS) {
                if (!slot_array[currentSlotIndex].empty) {
                    assert(slot_array[currentSlotIndex].offset != INVALID_VALUE);
                    const char* tuple_data = page_buffer + slot_array[currentSlotIndex].offset;
                    std::istringstream iss(std::string(tuple_data, slot_array[currentSlotIndex].length));
                    currentTuple = Tuple::deserialize(iss);
                    if(scan_relation.length() > 0
                        && currentTuple->fields.back()->asString() != scan_relation){
                            currentSlotIndex++;
                            continue;
                    }
                    currentSlotIndex++; // Move to the next slot for the next call
                    return; // Tuple loaded successfully
                }
                currentSlotIndex++;
            }

            // Increment page index after exhausting current page
            currentPageIndex++;
        }

        // No more tuples are available
        currentTuple.reset();
    }
};


class IPredicate {
public:
    virtual ~IPredicate() = default;
    virtual bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const = 0;
};


void printTuple(const std::vector<std::unique_ptr<Field>>& tupleFields) {
    std::cout << "Tuple: [";
    for (const auto& field : tupleFields) {
        field->print(); // Assuming `print()` is a method that prints field content
        std::cout << " ";
    }
    std::cout << "]"<<std::endl;
}


// Helper enum for storing comparison operators
enum class ComparisonOperator {
    EQ,
    NE,
    GT,
    GE,
    LT,
    LE
};


class SimplePredicate: public IPredicate {
public:
    enum OperandType { DIRECT, INDIRECT };

    struct Operand {
        std::unique_ptr<Field> directValue;
        size_t index = 0;
        OperandType type;

        Operand(std::unique_ptr<Field> value) : directValue(std::move(value)), type(DIRECT) {}
        Operand(size_t idx) : index(idx), type(INDIRECT) {}
    };

    Operand left_operand;
    Operand right_operand;
    ComparisonOperator comparison_operator;

    SimplePredicate(Operand left, Operand right, ComparisonOperator op)
        : left_operand(std::move(left)), right_operand(std::move(right)), comparison_operator(op) {}

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        const Field* leftField = nullptr;
        const Field* rightField = nullptr;

        if (left_operand.type == DIRECT) {
            leftField = left_operand.directValue.get();
        } else if (left_operand.type == INDIRECT) {
            leftField = tupleFields[left_operand.index].get();
        }

        if (right_operand.type == DIRECT) {
            rightField = right_operand.directValue.get();
        } else if (right_operand.type == INDIRECT) {
            rightField = tupleFields[right_operand.index].get();
        }

        if (leftField == nullptr || rightField == nullptr) {
            std::cerr << "Error: Invalid field reference.\n";
            return false;
        }

        if (leftField->getType() != rightField->getType()) {
            std::cerr << "Comparing fields of different types: " << std::endl;
            return false;
        }

        // Perform comparison based on field type
        switch (leftField->getType()) {
            case FieldType::INT: {
                int left_val = leftField->asInt();
                int right_val = rightField->asInt();
                return compare(left_val, right_val);
            }
            case FieldType::FLOAT: {
                float left_val = leftField->asFloat();
                float right_val = rightField->asFloat();
                return compare(left_val, right_val);
            }
            case FieldType::STRING: {
                std::string left_val = leftField->asString();
                std::string right_val = rightField->asString();
                return compare(left_val, right_val);
            }
            default:
                std::cerr << "Invalid field type\n";
                return false;
        }
    }


private:

    // Compares two values of the same type
    template<typename T>
    bool compare(const T& left_val, const T& right_val) const {
        switch (comparison_operator) {
            case ComparisonOperator::EQ: return left_val == right_val;
            case ComparisonOperator::NE: return left_val != right_val;
            case ComparisonOperator::GT: return left_val > right_val;
            case ComparisonOperator::GE: return left_val >= right_val;
            case ComparisonOperator::LT: return left_val < right_val;
            case ComparisonOperator::LE: return left_val <= right_val;
            default: std::cerr << "Invalid predicate type\n"; return false;
        }
    }
};


class ComplexPredicate : public IPredicate {
public:
    enum LogicOperator { AND, OR };

private:
    std::vector<std::unique_ptr<IPredicate>> predicates;
    LogicOperator logic_operator;

public:
    ComplexPredicate(LogicOperator op) : logic_operator(op) {}

    void addPredicate(std::unique_ptr<IPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        
        if (logic_operator == AND) {
            for (const auto& pred : predicates) {
                if (!pred->check(tupleFields)) {
                    return false; // If any predicate fails, the AND condition fails
                }
            }
            return true; // All predicates passed
        } else if (logic_operator == OR) {
            for (const auto& pred : predicates) {
                if (pred->check(tupleFields)) {
                    return true; // If any predicate passes, the OR condition passes
                }
            }
            return false; // No predicates passed
        }
        return false;
    }
};


class SelectOperator : public UnaryOperator {
private:
    std::unique_ptr<IPredicate> predicate;
    bool has_next;
    std::vector<std::unique_ptr<Field>> currentOutput; // Store the current output here

public:
    SelectOperator(Operator& input, std::unique_ptr<IPredicate> predicate)
        : UnaryOperator(input), predicate(std::move(predicate)), has_next(false) {}

    void open() override {
        input->open();
        has_next = false;
        currentOutput.clear(); // Ensure currentOutput is cleared at the beginning
    }

    bool next() override {
        while (input->next()) {
            const auto& output = input->getOutput(); // Temporarily hold the output
            if (predicate->check(output)) {
                // If the predicate is satisfied, store the output in the member variable
                currentOutput.clear(); // Clear previous output
                for (const auto& field : output) {
                    // Assuming Field class has a clone method or copy constructor to duplicate fields
                    currentOutput.push_back(field->clone());
                }
                has_next = true;
                return true;
            }
        }
        has_next = false;
        currentOutput.clear(); // Clear output if no more tuples satisfy the predicate
        return false;
    }

    void close() override {
        input->close();
        currentOutput.clear(); // Ensure currentOutput is cleared at the end
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (has_next) {
            // Since currentOutput already holds the desired output, simply return it
            // Need to create a deep copy to return since we're returning by value
            std::vector<std::unique_ptr<Field>> outputCopy;
            for (const auto& field : currentOutput) {
                outputCopy.push_back(field->clone()); // Clone each field
            }
            return outputCopy;
        } else {
            return {}; // Return an empty vector if no matching tuple is found
        }
    }
};


enum class AggrFuncType { COUNT, MAX, MIN, SUM };


struct AggrFunc {
    AggrFuncType func;
    size_t attr_index; // Index of the attribute to aggregate
};


bool operator!=(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    /*----------------*/
    return !(lhs == rhs);
    /*----------------*/
}

bool operator<(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    /*----------------*/
    if (lhs.type != rhs.type) {
        throw std::runtime_error("Attempted to compare Fields of different types with '<'.");
    }

    switch (lhs.type) {
        case FieldType::INT: {
            int l = *reinterpret_cast<const int*>(lhs.data.get());
            int r = *reinterpret_cast<const int*>(rhs.data.get());
            return l < r;
        }
        case FieldType::FLOAT: {
            float l = *reinterpret_cast<const float*>(lhs.data.get());
            float r = *reinterpret_cast<const float*>(rhs.data.get());
            return l < r;
        }
        case FieldType::STRING: {
            // Lexicographic order for strings.
            std::string l(lhs.data.get(), lhs.data_length ? lhs.data_length - 1 : 0);
            std::string r(rhs.data.get(), rhs.data_length ? rhs.data_length - 1 : 0);
            return l < r;
        }
        default:
            throw std::runtime_error("Unsupported FieldType in operator<.");
    }
    /*----------------*/
}

bool operator>(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    /*----------------*/
    return (rhs < lhs);
    /*----------------*/
}

bool operator<=(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    /*----------------*/
    return !(lhs > rhs);
    /*----------------*/
}

bool operator>=(const Field& lhs, const Field& rhs) {
    // TODO: Add your implementation here
    // UNUSED(lhs);
    // UNUSED(rhs);
    // return false;
    /*----------------*/
    return !(lhs < rhs);
    /*----------------*/
}


class PrintOperator : public UnaryOperator {
private:
    // TODO: Add your implementation here
    std::ostream& stream;
public:
    // PrintOperator(Operator& input, std::ostream& stream) 
    //     : UnaryOperator(input) {
    //         // TODO: Add your implementation here
    //         /*----------------*/
    //         //UNUSED(stream);
    //         /*----------------*/
    //         this->stream = &stream;
    //     }
    PrintOperator(Operator& input, std::ostream& stream)
        : UnaryOperator(input), stream(stream) {
        // nothing else needed here
    }
        

    void open() override {
        // TODO: Add your implementation here
        input->open();
    }

    bool next() override {
        // TODO: Add your implementation here
        //return false;
        if (!input->next()) {
            return false;
        }

        // Get current tuple and print it
        auto row = input->getOutput();
        for (std::size_t i = 0; i < row.size(); ++i) {
            if (i > 0) {
                stream << ", ";
            }
            if (row[i]) {
                stream << row[i]->asString();
            }
        }
        stream << std::endl;  // matches sutdentsRelationToString()
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input->close();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // No output to return
    }
};


class ProjectOperator : public UnaryOperator {
    private:
        // TODO: Add your implementation here
        std::vector<size_t> attr_indexes; // indices of attributes to keep (0-based)
        bool has_next = false;
        std::vector<std::unique_ptr<Field>> currentOutput; // projected current tuple

    public:
        // ProjectOperator(Operator& input, std::vector<size_t> attr_indexes)
        //     : UnaryOperator(input) {
        //         // TODO: Add your implementation here
        //         UNUSED(attr_indexes);
        //     }
        ProjectOperator(Operator& input, std::vector<size_t> attr_indexes)
            : UnaryOperator(input), attr_indexes(std::move(attr_indexes)) {}

        ~ProjectOperator() = default;

        void open() override {
            // TODO: Add your implementation here
            input->open();
            has_next = false;
            currentOutput.clear();
        }

        bool next() override {
            // TODO: Add your implementation here
            //return false;
            while (input->next()) {
                const auto& inputTuple = input->getOutput();
                // Build projected tuple
                currentOutput.clear();
                currentOutput.reserve(attr_indexes.size());
                for (size_t idx : attr_indexes) {
                    if (idx >= inputTuple.size()) {
                        throw std::out_of_range("ProjectOperator: attribute index out of range");
                    }
                    currentOutput.push_back(inputTuple[idx]->clone());
                }
                has_next = true;
                return true;
            }
            has_next = false;
            currentOutput.clear();
            return false;
        }

        void close() override {
            // TODO: Add your implementation here
            input->close();
            currentOutput.clear();
            has_next = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            // TODO: Add your implementation here
            // return {};
            if (!has_next) {
                return {};
            }
            std::vector<std::unique_ptr<Field>> out;
            out.reserve(currentOutput.size());
            for (const auto& field : currentOutput) {
                out.push_back(field->clone());
            }
            return out;
        }
};


class Sort : public UnaryOperator {
public:
    struct Criterion {
        size_t attr_index;
        bool desc;
    };

private:
    // TODO: Add your implementation here
    std::vector<Criterion> criteria;
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

public:
    // Sort(Operator& input, std::vector<Criterion> criteria)
    //     : UnaryOperator(input) {
    //         // TODO: Add your implementation here
    //         UNUSED(criteria);
    //     }
    Sort(Operator& input, std::vector<Criterion> criteria)
        : UnaryOperator(input), criteria(std::move(criteria)) {}

    ~Sort() override = default;

    void open() override {
        // TODO: Add your implementation here
        input->open();
        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();

        while (input->next()) {
            auto row = input->getOutput();           // vector<unique_ptr<Field>>
            tuples.push_back(std::move(row));        // store it
        }

        // Done with the child for this operator
        //input->close();

        // Sort tuples based on the criteria
        std::sort(
            tuples.begin(),
            tuples.end(),
            [this](const std::vector<std::unique_ptr<Field>>& a,
                   const std::vector<std::unique_ptr<Field>>& b) {
                for (const auto& crit : criteria) {
                    size_t idx = crit.attr_index;
                    if (idx >= a.size() || idx >= b.size()) {
                        throw std::out_of_range("Sort criterion index out of range");
                    }
                    const Field& fa = *a[idx];
                    const Field& fb = *b[idx];

                    if (fa == fb) {
                        // equal on this criterion; move to the next one
                        continue;
                    }

                    // desc == true => descending, false => ascending
                    if (crit.desc) {
                        // descending: bigger first
                        return fb < fa;
                    } else {
                        // ascending: smaller first
                        return fa < fb;
                    }
                }
                // all criteria equal => keep original order
                return false;
            }
        );
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        // Prepare currentOutput for this position
        currentOutput.clear();
        const auto& src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto& field : src) {
            currentOutput.push_back(field->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input->close();
        tuples.clear();
        currentOutput.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        //return {};
        if (!has_next) {
            return {};
        }

        // Deep copy currentOutput (same pattern as Select/Project)
        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& field : currentOutput) {
            out.push_back(field->clone());
        }
        return out;
    }
};
    

/// Computes the inner equi-join of the two inputs on one attribute.
class HashJoin : public BinaryOperator {
private:
    // TODO: Add your implementation here
    std::unordered_map<Field, std::vector<std::vector<std::unique_ptr<Field>>>, FieldHasher> hash_table;
    size_t left_attr_index;
    size_t right_attr_index;

    // Probe-side (right) state
    std::vector<std::unique_ptr<Field>> currentRightRow;
    const std::vector<std::vector<std::unique_ptr<Field>>>* currentLeftMatches = nullptr;
    size_t currentLeftMatchIndex = 0;

    // Output state
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

public:
    // HashJoin(Operator& left_input, Operator& right_input, size_t left_attr_index, size_t right_attr_index)
    //     : BinaryOperator(left_input, right_input) {
    //         // TODO: Add your implementation here
    //         UNUSED(left_attr_index);
    //         UNUSED(right_attr_index);
    //     }
    HashJoin(Operator& left_input, Operator& right_input, size_t left_attr_index, size_t right_attr_index)
        : BinaryOperator(left_input, right_input),
          left_attr_index(left_attr_index),
          right_attr_index(right_attr_index) {}

    ~HashJoin() override = default;

    void open() override {
        // TODO: Add your implementation here
        input_left->open();
        input_right->open();

        // Build hash table from all tuples of the left input
        hash_table.clear();
        currentLeftMatches = nullptr;
        currentLeftMatchIndex = 0;
        currentOutput.clear();
        has_next = false;
        currentRightRow.clear();

        while (input_left->next()) {
            auto leftRow = input_left->getOutput(); // vector<unique_ptr<Field>>

            if (left_attr_index >= leftRow.size()) {
                throw std::out_of_range("HashJoin: left_attr_index out of range");
            }

            // Copy join key field for map key
            Field key(*leftRow[left_attr_index]);

            // Insert the row into the hash table (move the row to avoid copies)
            hash_table[key].push_back(std::move(leftRow));
        }
    }

    bool next() override {
        // TODO: Add your implementation here
        //return false;
        while (true) {
            if (currentLeftMatches != nullptr && currentLeftMatchIndex < currentLeftMatches->size()) {
                const auto& leftRow = (*currentLeftMatches)[currentLeftMatchIndex++];

                // Build joined tuple: [left_columns..., right_columns...]
                currentOutput.clear();
                currentOutput.reserve(leftRow.size() + currentRightRow.size());

                for (const auto& f : leftRow) {
                    currentOutput.push_back(f->clone());
                }
                for (const auto& f : currentRightRow) {
                    currentOutput.push_back(f->clone());
                }

                has_next = true;
                return true;
            }

            // Otherwise, fetch the next right row
            if (!input_right->next()) {
                // No more right tuples -> join complete
                has_next = false;
                currentOutput.clear();
                return false;
            }

            currentRightRow = input_right->getOutput();

            if (right_attr_index >= currentRightRow.size()) {
                throw std::out_of_range("HashJoin: right_attr_index out of range");
            }

            Field rightKey(*currentRightRow[right_attr_index]);
            auto it = hash_table.find(rightKey);
            if (it == hash_table.end() || it->second.empty()) {
                // No match for this right row; continue to next right
                currentLeftMatches = nullptr;
                currentLeftMatchIndex = 0;
                continue;
            }

            // We have matches for this right row; start emitting them
            currentLeftMatches = &it->second;
            currentLeftMatchIndex = 0;
            // loop back; will emit on next iteration of while(true)
        }
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        hash_table.clear();
        currentRightRow.clear();
        currentLeftMatches = nullptr;
        currentLeftMatchIndex = 0;
        currentOutput.clear();
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};

    
/// Groups and calculates (potentially multiple) aggregates on the input.
class HashAggregationOperator : public UnaryOperator {
private:
    // TODO: Add your implementation here
    std::vector<size_t> groupByAttrs;
    std::vector<AggrFunc> aggrFuncs;
    std::vector<size_t> selectAttrs;  // which original columns to copy from first tuple

    struct GroupState {
        bool initialized = false;
        std::vector<std::unique_ptr<Field>> firstTuple; // full first tuple of group
        std::vector<Field> aggrValues;                  // aggregate values as Fields
    };

    std::unordered_map<std::string, GroupState> groups;
    std::vector<std::vector<std::unique_ptr<Field>>> outputTuples;

    size_t currentIndex = 0;
    bool hasNext = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

    // Build key from group-by attributes (or constant for global aggregation)
    std::string make_key(const std::vector<std::unique_ptr<Field>>& row) const {
        if (groupByAttrs.empty()) {
            return "GLOBAL_AGG_KEY";
        }
        std::string key;
        for (size_t i = 0; i < groupByAttrs.size(); ++i) {
            size_t idx = groupByAttrs[i];
            if (i > 0) key.push_back('|');
            key += row[idx]->asString();
        }
        return key;
    }

public:
    HashAggregationOperator(Operator& input,
                            const std::vector<size_t>& group_by_attrs,
                            const std::vector<AggrFunc>& aggr_funcs,
                            const std::vector<size_t>& select_attrs)
        : UnaryOperator(input),
          groupByAttrs(group_by_attrs),
          aggrFuncs(aggr_funcs),
          selectAttrs(select_attrs) {}

    void open() override {
        input->open();

        groups.clear();
        outputTuples.clear();
        currentIndex = 0;
        hasNext = false;
        currentOutput.clear();

        // Consume all input rows
        while (input->next()) {
            auto row = input->getOutput();
            std::string key = make_key(row);
            GroupState& st = groups[key];

            if (!st.initialized) {
                // Store full first tuple for this group
                st.firstTuple.clear();
                st.firstTuple.reserve(row.size());
                for (const auto& f : row) {
                    st.firstTuple.push_back(f->clone());
                }

                // Initialize aggregates
                st.aggrValues.clear();
                st.aggrValues.reserve(aggrFuncs.size());
                for (const auto& ag : aggrFuncs) {
                    switch (ag.func) {
                        case AggrFuncType::COUNT:
                            st.aggrValues.emplace_back(Field(0));
                            break;
                        case AggrFuncType::SUM: {
                            const Field& v = *row[ag.attr_index];
                            if (v.type == FieldType::INT) {
                                st.aggrValues.emplace_back(Field(0));
                            } else if (v.type == FieldType::FLOAT) {
                                st.aggrValues.emplace_back(Field(0.0f));
                            } else {
                                st.aggrValues.emplace_back(Field(0));
                            }
                            break;
                        }
                        case AggrFuncType::MIN:
                        case AggrFuncType::MAX: {
                            const Field& v = *row[ag.attr_index];
                            st.aggrValues.emplace_back(v);
                            break;
                        }
                    }
                }

                st.initialized = true;
            }

            // Update aggregates with this row
            for (size_t i = 0; i < aggrFuncs.size(); ++i) {
                const auto& ag = aggrFuncs[i];
                Field& aggVal = st.aggrValues[i];
                const Field& v = *row[ag.attr_index];

                switch (ag.func) {
                    case AggrFuncType::COUNT: {
                        int cur = aggVal.asInt();
                        aggVal = Field(cur + 1);
                        break;
                    }
                    case AggrFuncType::SUM: {
                        if (v.type == FieldType::INT) {
                            int cur = aggVal.asInt();
                            aggVal = Field(cur + v.asInt());
                        } else if (v.type == FieldType::FLOAT) {
                            float cur = aggVal.asFloat();
                            aggVal = Field(cur + v.asFloat());
                        }
                        break;
                    }
                    case AggrFuncType::MIN: {
                        if (v < aggVal) {
                            aggVal = v;
                        }
                        break;
                    }
                    case AggrFuncType::MAX: {
                        if (aggVal < v) {
                            aggVal = v;
                        }
                        break;
                    }
                }
            }
        }

        // Build final output tuples: [selected_columns_from_first_tuple..., aggregate_values...]
        for (auto& kv : groups) {
            GroupState& st = kv.second;
            if (!st.initialized) continue;

            std::vector<std::unique_ptr<Field>> out;

            if (!selectAttrs.empty()) {
                // Use SELECT list
                for (size_t idx : selectAttrs) {
                    out.push_back(st.firstTuple[idx]->clone());
                }
            } else if (!groupByAttrs.empty()) {
                // No explicit SELECT attributes -> use group-by columns
                for (size_t idx : groupByAttrs) {
                    out.push_back(st.firstTuple[idx]->clone());
                }
            }

            // Append aggregates
            for (const auto& aggVal : st.aggrValues) {
                out.push_back(std::make_unique<Field>(aggVal));
            }

            outputTuples.push_back(std::move(out));
        }

        currentIndex = 0;
    }

    bool next() override {
        if (currentIndex >= outputTuples.size()) {
            hasNext = false;
            currentOutput.clear();
            return false;
        }

        currentOutput.clear();
        const auto& src = outputTuples[currentIndex++];
        currentOutput.reserve(src.size());
        for (const auto& f : src) {
            currentOutput.push_back(f->clone());
        }
        hasNext = true;
        return true;
    }

    void close() override {
        input->close();
        groups.clear();
        outputTuples.clear();
        currentOutput.clear();
        currentIndex = 0;
        hasNext = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (!hasNext) {
            return {};
        }
        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};
    

/// Computes the union of the two inputs with set semantics.
class UnionOperator : public BinaryOperator {
private:
    // TODO: Add your implementation here
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;

    // For iterating over the result
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

    // For duplicate detection (set semantics)
    std::unordered_set<std::string> seen;

    // Helper: turn a tuple into a unique string key
    std::string make_key(const std::vector<std::unique_ptr<Field>>& row) {
        std::string key;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) {
                key.push_back('\x1F'); // unit separator to avoid collisions
            }
            key += row[i]->asString();
        }
        return key;
    }

public:
    UnionOperator(Operator& left_input, Operator& right_input)
        : BinaryOperator(left_input, right_input) {}

    ~UnionOperator() = default;

    void open() override {
        // TODO: Add your implementation here
        input_left->open();
        input_right->open();

        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();
        seen.clear();

        // Helper lambda to consume one input and add unique rows
        auto process_input = [this](Operator* op) {
            while (op->next()) {
                auto row = op->getOutput(); // vector<unique_ptr<Field>>

                // Compute key for duplicate detection
                std::string key = make_key(row);

                // Insert only if not seen before
                if (seen.insert(key).second) {
                    tuples.push_back(std::move(row));
                }
            }
        };

        // Process left then right input
        process_input(input_left);
        process_input(input_right);
    }

    bool next() override {
        // TODO: Add your implementation here
        //return false;
        if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        // Prepare output for this position
        currentOutput.clear();
        const auto& src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto& f : src) {
            currentOutput.push_back(f->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        tuples.clear();
        currentOutput.clear();
        seen.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};


/// Computes the union of the two inputs with bag semantics.
class UnionAll: public BinaryOperator {
private:
    // TODO: Add your implementation here
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;
public:
    UnionAll(Operator& left_input, Operator& right_input)
        : BinaryOperator(left_input, right_input) {}

    ~UnionAll() = default;

    void open() override {
        // TODO: Add your implementation here
        // Open both inputs
        input_left->open();
        input_right->open();

        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();

        // Read everything from left, then from right (bag semantics)
        auto process_input = [this](Operator* op) {
            while (op->next()) {
                auto row = op->getOutput();
                tuples.push_back(std::move(row));
            }
        };

        process_input(input_left);
        process_input(input_right);
    }

    bool next() override {
        // TODO: Add your implementation here
        //return false;
        if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        currentOutput.clear();
        const auto& src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto& f : src) {
            currentOutput.push_back(f->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        tuples.clear();
        currentOutput.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        //return {};
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};


/// Computes the intersection of the two inputs with set semantics.
class Intersect: public BinaryOperator {
private:
    // TODO: Add your implementation here
    // Materialized intersection result (set semantics: no duplicates)
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

    // Keys seen in left input and intersection to handle set semantics
    std::unordered_set<std::string> left_keys;
    std::unordered_set<std::string> intersection_keys;

    // Helper: build a unique key for a tuple based on its field string values
    std::string make_key(const std::vector<std::unique_ptr<Field>>& row) {
        std::string key;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) {
                key.push_back('|'); // simple separator to avoid collisions
            }
            key += row[i]->asString();
        }
        return key;
    }

public:
    Intersect(Operator& left_input, Operator& right_input)
        : BinaryOperator(left_input, right_input) {}

    ~Intersect() = default;

    void open() override {
        // TODO: Add your implementation here
        // Open both children
        input_left->open();
        input_right->open();

        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();
        left_keys.clear();
        intersection_keys.clear();

        // Consume left input and remember its keys
        while (input_left->next()) {
            auto row = input_left->getOutput();
            std::string key = make_key(row);
            left_keys.insert(std::move(key));
        }

        // Consume right input and keep rows that also appear in left (set semantics)
        while (input_right->next()) {
            auto row = input_right->getOutput();
            std::string key = make_key(row);

            if (left_keys.find(key) != left_keys.end()) {
                // Only keep first occurrence for set semantics
                if (intersection_keys.insert(key).second) {
                    tuples.push_back(std::move(row));
                }
            }
        }
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        currentOutput.clear();
        const auto& src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto& f : src) {
            currentOutput.push_back(f->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        tuples.clear();
        currentOutput.clear();
        left_keys.clear();
        intersection_keys.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};


/// Computes the intersection of the two inputs with bag semantics.
class IntersectAll: public BinaryOperator {
private:
    // TODO: Add your implementation here
    // Materialized bag intersection result
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

    // Counts of tuples from the left input
    std::unordered_map<std::string, size_t> left_counts;

    // Helper: build a key for a tuple
    std::string make_key(const std::vector<std::unique_ptr<Field>>& row) {
        std::string key;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) {
                key.push_back('|'); // separator
            }
            key += row[i]->asString();
        }
        return key;
    }

public:
    IntersectAll(Operator& left_input, Operator& right_input)
        : BinaryOperator(left_input, right_input) {}

    ~IntersectAll() = default;

    void open() override {
        // TODO: Add your implementation here
        // Open both inputs
        input_left->open();
        input_right->open();

        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();
        left_counts.clear();

        // 1) Read all tuples from left and count them
        while (input_left->next()) {
            auto row = input_left->getOutput();
            std::string key = make_key(row);
            left_counts[key] += 1;
        }

        // 2) Read tuples from right; add if present in left_counts (bag semantics)
        while (input_right->next()) {
            auto row = input_right->getOutput();
            std::string key = make_key(row);

            auto it = left_counts.find(key);
            if (it != left_counts.end() && it->second > 0) {
                // Emit one copy and decrement remaining count
                tuples.push_back(std::move(row));
                it->second -= 1;
            }
        }
    }

    bool next() override {
        // TODO: Add your implementation here
        // return false;
        if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        currentOutput.clear();
        const auto& src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto& f : src) {
            currentOutput.push_back(f->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        tuples.clear();
        currentOutput.clear();
        left_counts.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        // return {};
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};


/// Computes input_left - input_right with set semantics.
class Except: public BinaryOperator {
private:
    // TODO: Add your implementation here
    // Materialized set difference result: left \ right (no duplicates)
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

    // Key sets
    std::unordered_set<std::string> right_keys;
    std::unordered_set<std::string> result_keys;

    // Helper to create a key from a tuple
    std::string make_key(const std::vector<std::unique_ptr<Field>>& row) {
        std::string key;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) key.push_back('|');   // separator
            key += row[i]->asString();
        }
        return key;
    }

public:
    Except(Operator& left_input, Operator& right_input)
        : BinaryOperator(left_input, right_input) {}

    ~Except() = default;

    void open() override {
        // TODO: Add your implementation here
        // Open both children
        input_left->open();
        input_right->open();

        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();
        right_keys.clear();
        result_keys.clear();

        // 1. Consume all right tuples and record their keys
        while (input_right->next()) {
            auto row = input_right->getOutput();
            std::string key = make_key(row);
            right_keys.insert(std::move(key));
        }

        // 2. Consume left tuples; keep those whose key is NOT in right_keys
        //    and emit each distinct key at most once (set semantics).
        while (input_left->next()) {
            auto row = input_left->getOutput();
            std::string key = make_key(row);

            if (right_keys.find(key) == right_keys.end()) {
                // Only keep the first occurrence of each distinct key
                if (result_keys.insert(key).second) {
                    tuples.push_back(std::move(row));
                }
            }
        }
    }

    bool next() override {
        // TODO: Add your implementation here
         if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        currentOutput.clear();
        const auto& src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto& f : src) {
            currentOutput.push_back(f->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        tuples.clear();
        currentOutput.clear();
        right_keys.clear();
        result_keys.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto& f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};


/// Computes input_left - input_right with bag semantics.
class ExceptAll: public BinaryOperator {
private:
    // TODO: Add your implementation here
    // Result tuples after bag difference
    std::vector<std::vector<std::unique_ptr<Field>>> tuples;
    size_t current_index = 0;
    bool has_next = false;
    std::vector<std::unique_ptr<Field>> currentOutput;

    // Store remaining left rows keyed by their value
    std::unordered_map<std::string,
        std::vector<std::vector<std::unique_ptr<Field>>>> left_rows_by_key;

    // Build a string key for a tuple
    std::string make_key(const std::vector<std::unique_ptr<Field>>& row) {
        std::string key;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) key.push_back('|'); // separator
            key += row[i]->asString();
        }
        return key;
    }

public:
    ExceptAll(Operator& left_input, Operator& right_input)
        : BinaryOperator(left_input, right_input) {}

    ~ExceptAll() = default;

    void open() override {
        // TODO: Add your implementation here
        input_left->open();
        input_right->open();

        tuples.clear();
        current_index = 0;
        has_next = false;
        currentOutput.clear();
        left_rows_by_key.clear();

        // 1) Bucket all left tuples by key (keep every copy)
        while (input_left->next()) {
            auto row = input_left->getOutput(); // vector<unique_ptr<Field>>
            std::string key = make_key(row);
            left_rows_by_key[key].push_back(std::move(row));
        }

        // 2) For each right tuple, remove one matching left tuple (if any)
        while (input_right->next()) {
            auto row = input_right->getOutput();
            std::string key = make_key(row);

            auto it = left_rows_by_key.find(key);
            if (it != left_rows_by_key.end() && !it->second.empty()) {
                it->second.pop_back(); // cancel out one occurrence
            }
        }

        // 3) Whatever left_rows_by_key still contains is the bag-difference
        for (auto &kv : left_rows_by_key) {
            auto &vec = kv.second;
            for (auto &stored_row : vec) {
                tuples.push_back(std::move(stored_row));
            }
            vec.clear();
        }
        left_rows_by_key.clear();
    }

    bool next() override {
        // TODO: Add your implementation here
        if (current_index >= tuples.size()) {
            has_next = false;
            currentOutput.clear();
            return false;
        }

        currentOutput.clear();
        const auto &src = tuples[current_index++];
        currentOutput.reserve(src.size());
        for (const auto &f : src) {
            currentOutput.push_back(f->clone());
        }
        has_next = true;
        return true;
    }

    void close() override {
        // TODO: Add your implementation here
        input_left->close();
        input_right->close();

        tuples.clear();
        currentOutput.clear();
        left_rows_by_key.clear();
        current_index = 0;
        has_next = false;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // TODO: Add your implementation here
        if (!has_next) {
            return {};
        }

        std::vector<std::unique_ptr<Field>> out;
        out.reserve(currentOutput.size());
        for (const auto &f : currentOutput) {
            out.push_back(f->clone());
        }
        return out;
    }
};


class InsertOperator : public Operator {
private:
    BufferManager& bufferManager;
    std::unique_ptr<Tuple> tupleToInsert;

public:
    InsertOperator(BufferManager& manager) : bufferManager(manager) {}

    // Set the tuple to be inserted by this operator.
    void setTupleToInsert(std::unique_ptr<Tuple> tuple) {
        tupleToInsert = std::move(tuple);
    }

    void open() override {
        // Not used in this context
    }

    bool next() override {
        if (!tupleToInsert) return false; // No tuple to insert

        for (size_t pageId = 0; pageId < bufferManager.getNumPages(); ++pageId) {
            auto& page = bufferManager.getPage(pageId);
            // Attempt to insert the tuple
            if (page->addTuple(tupleToInsert->clone())) { 
                // Flush the page to disk after insertion
                bufferManager.flushPage(pageId); 
                return true; // Insertion successful
            }
        }

        // If insertion failed in all existing pages, extend the database and try again
        bufferManager.extend();
        auto& newPage = bufferManager.getPage(bufferManager.getNumPages() - 1);
        if (newPage->addTuple(tupleToInsert->clone())) {
            bufferManager.flushPage(bufferManager.getNumPages() - 1);
            return true; // Insertion successful after extending the database
        }

        return false; // Insertion failed even after extending the database
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};

class DeleteOperator : public Operator {
private:
    BufferManager& bufferManager;
    size_t pageId;
    size_t tupleId;

public:
    DeleteOperator(BufferManager& manager, size_t pageId, size_t tupleId) 
        : bufferManager(manager), pageId(pageId), tupleId(tupleId) {}

    void open() override {
        // Not used in this context
    }

    bool next() override {
        auto& page = bufferManager.getPage(pageId);
        if (!page) {
            std::cerr << "Page not found." << std::endl;
            return false;
        }

        page->deleteTuple(tupleId); // Perform deletion
        bufferManager.flushPage(pageId); // Flush the page to disk after deletion
        return true;
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};


// Structure to represent a single WHERE condition
struct WhereCondition {
    size_t attributeIndex;
    ComparisonOperator op;
    Field value;
    
    WhereCondition(size_t idx, ComparisonOperator o, Field v) 
        : attributeIndex(idx), op(o), value(std::move(v)) {}
};


// Query Executor functions
struct QueryComponents {
    std::vector<size_t> selectAttributes;
    
    // Aggregate functions support
    std::vector<AggrFunc> aggregateFunctions;
    std::vector<size_t> groupByAttributes;
    bool sumOperation = false;
    int sumAttributeIndex = -1;
    bool groupBy = false;
    int groupByAttributeIndex = -1;
    
    // WHERE conditions
    std::vector<WhereCondition> whereConditions;
    
    bool whereCondition = false;
    int whereAttributeIndex = -1;
    int lowerBound = std::numeric_limits<int>::min();
    int upperBound = std::numeric_limits<int>::max();
    bool hasLowerBound = false;
    bool hasUpperBound = false;
    
    bool innerJoin = false;
    int joinAttributeIndex1 = -1;
    int joinAttributeIndex2 = -1;
    std::string relation;
    std::string joinRelation;
    bool orderBy = false;
    int orderByAttributeIndex = -1;
};


static inline std::string trim_inplace(const std::string& s) {
    std::string trimmed = s;
    // whitespace set includes space, tab, newline, carriage return, form feed, vertical tab
    const char* ws = " \t\n\r\f\v";
    const auto start = trimmed.find_first_not_of(ws);
    if (start == std::string::npos) {
        trimmed.clear();
        return trimmed;
    }
    const auto end = trimmed.find_last_not_of(ws);
    trimmed = trimmed.substr(start, end - start + 1);
    return trimmed;
}

// Helper function to parse field value from string
Field parseFieldValue(const std::string& value) {
    if (value.empty() || value == "None") {
        return Field("");  // Default to STRING for empty values
    }

    std::string trimmed = trim_inplace(value);

    // Check if it's a quoted string (single or double quotes)
    if ((trimmed.front() == '"' && trimmed.back() == '"') || 
        (trimmed.front() == '\'' && trimmed.back() == '\'')) {
        return Field(trimmed.substr(1, trimmed.length() - 2));
    }
    
    std::regex intPattern("^[+-]?\\d+$");
    std::regex floatPattern("^[+-]?\\d*\\.\\d+$");

    if (std::regex_match(trimmed, intPattern)) {
        return Field(std::stoi(trimmed));
    } else if (std::regex_match(trimmed, floatPattern)) {
        return Field(std::stof(trimmed));
    }
    
    // Otherwise treat as string
    return Field(trimmed);
}

// Helper function to parse comparison operator from string
ComparisonOperator parseOperator(const std::string& op) {
    if (op == "==") return ComparisonOperator::EQ;
    if (op == "!=") return ComparisonOperator::NE;
    if (op == ">=") return ComparisonOperator::GE;
    if (op == "<=") return ComparisonOperator::LE;
    if (op == ">") return ComparisonOperator::GT;
    if (op == "<") return ComparisonOperator::LT;
    throw std::runtime_error("Invalid comparison operator: " + op);
}

void prettyPrint(const QueryComponents& components) {
    std::cout << "Query Components:\n";
    std::cout << "  Selected Attributes: ";
    for (auto attr : components.selectAttributes) {
        std::cout << "{" << attr + 1 << "} "; // Convert back to 1-based indexing for display
    }
    std::cout << "\n  SUM Operation: " << (components.sumOperation ? "Yes" : "No");
    if (components.sumOperation) {
        std::cout << " on {" << components.sumAttributeIndex + 1 << "}";
    }
    std::cout << "\n  GROUP BY: " << (components.groupBy ? "Yes" : "No");
    if (components.groupBy) {
        std::cout << " on {" << components.groupByAttributeIndex + 1 << "}";
    }
    std::cout << "\n  WHERE Condition: " << (components.whereCondition ? "Yes" : "No");
    if (components.whereCondition) {
        std::cout << " on {" << components.whereAttributeIndex + 1 << "} > " << components.lowerBound << " and < " << components.upperBound;
    }
    std::cout << std::endl;
}

// Parse a CSV line respecting quoted fields
std::unique_ptr<Tuple> parseCSVLine(const std::string& line) {
    auto tuple = std::make_unique<Tuple>();
    std::string field;
    bool in_quotes = false;
    
    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];
        
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == ',' && !in_quotes) {
            tuple->addField(std::make_unique<Field>(parseFieldValue(field)));
            field.clear();
        } else {
            field += c;
        }
    }
    tuple->addField(std::make_unique<Field>(parseFieldValue(field)));
    return tuple;
}

// Schema for a relation
struct Relation {
    std::string name;
    std::vector<std::string> column_names;
    std::vector<FieldType> column_types;
    std::vector<std::unique_ptr<Tuple>> rows; // Store raw data as fields
    
    Relation() = default;
    Relation(const std::string& name) : name(name) {}
};


// Relation Manager - stores all loaded relations
class RelationManager {
private:
    QueryComponents components;
    BufferManager buffer_manager;
    std::unordered_map<std::string, Relation> relations;

public:
    // Load a CSV file and infer schema
    bool loadCSV(const std::string& file_path, const std::string& relation_name) {
        std::ifstream file(file_path);
        if (!file.is_open()) {
            std::cerr << "Error: Could not open file " << file_path << std::endl;
            return false;
        }
        
        std::string rel_name = relation_name;
        
        Relation schema(rel_name);
        
        // Read header
        std::string header_line;
        if (!std::getline(file, header_line)) {
            std::cerr << "Error: Empty file " << file_path << std::endl;
            return false;
        }
        
        auto tuple = parseCSVLine(header_line);
        schema.column_names.clear();
        for (const auto& field : tuple->fields) {
            schema.column_names.push_back(field->asString());
        }
        
        // Read all rows and infer types
        std::string line;
        
        while (std::getline(file, line)) {
            if (line.empty()) continue;

            auto tuple = parseCSVLine(line);
            if (tuple->fields.size() != schema.column_names.size()) {
                std::cerr << "Warning: Skipping malformed line with " << tuple->fields.size() 
                          << " fields (expected " << schema.column_names.size() << ")" << std::endl;
                continue;
            }

            schema.rows.push_back(std::move(tuple));
        }
        
		schema.column_types.clear();
		schema.column_types.resize(schema.column_names.size(), FieldType::STRING);
		for(size_t i = 0; i < schema.column_names.size(); i++) {
			schema.column_types[i] = schema.rows.back()->fields[i]->getType();
		}
 
        file.close();
        relations[rel_name] = std::move(schema);
        return true;
    }
    
    // Get a relation by name
    const Relation* getRelation(const std::string& name) const {
        auto it = relations.find(name);
        if (it != relations.end()) {
            return &it->second;
        }
        return nullptr;
    }
    
    // Check if a relation exists
    bool hasRelation(const std::string& name) const {
        return relations.find(name) != relations.end();
    }
        
    // Populate database from loaded relation
    bool populateDatabase(const std::string& relation_name) {
        auto rel = getRelation(relation_name);
        if (!rel) {
            std::cerr << "Relation '" << relation_name << "' not found" << std::endl;
            return false;
        }
        
        InsertOperator insert_operator(buffer_manager);
        size_t inserted_count = 0;
        
        for (const auto& row : rel->rows) {
            auto tuple = std::make_unique<Tuple>();
            
            for (size_t i = 0; i < row->fields.size(); ++i) {
                // Clone the field directly to preserve its type
                tuple->addField(row->fields[i]->clone());
            }

            // Add relation name as the last field (for relation filtering)
            tuple->addField(std::make_unique<Field>(relation_name));
            
            insert_operator.setTupleToInsert(std::move(tuple));
            if (insert_operator.next()) {
                inserted_count++;
            } else {
                std::cerr << "Warning: Failed to insert row " << inserted_count << std::endl;
                return false;
            }
        }
        insert_operator.close();
        return true;
    }

    void parseQuery(const std::string& query) {
        // Parse selected attributes
        components = QueryComponents{};


        std::regex selectRegex("SELECT \\{([\\d,\\s]+)\\}");
        std::smatch selectMatches;
        if (std::regex_search(query, selectMatches, selectRegex)) {
            std::string numbers = selectMatches[1].str();
            std::stringstream ss(numbers);
            std::string number;
            
            while (std::getline(ss, number, ',')) {
                if (!number.empty() && number != "*") {
                    components.selectAttributes.push_back(std::stoi(number) - 1);
                }
            }
        }
    
        // TODO: Add your implementation here for parsing 'FROM'
        // Parse FROM {relation_name}
        std::regex fromRegex(R"(FROM\s*\{([^}]+)\})");
        std::smatch fromMatches;
        if (std::regex_search(query, fromMatches, fromRegex)) {
            components.relation = fromMatches[1].str();
        }

        std::regex aggrRegex(R"((COUNT|SUM|MIN|MAX)\s*\{(\d+)\})");
        auto it  = std::sregex_iterator(query.begin(), query.end(), aggrRegex);
        auto end = std::sregex_iterator();

        for (; it != end; ++it) {
            std::smatch m = *it;
            std::string funcStr = m[1].str();
            size_t attrIndex = static_cast<size_t>(std::stoi(m[2].str()) - 1); // 1-based -> 0-based

            AggrFuncType ftype;
            if (funcStr == "COUNT")      ftype = AggrFuncType::COUNT;
            else if (funcStr == "SUM")   ftype = AggrFuncType::SUM;
            else if (funcStr == "MIN")   ftype = AggrFuncType::MIN;
            else                         ftype = AggrFuncType::MAX;

            components.aggregateFunctions.push_back(AggrFunc{ftype, attrIndex});
        }
        // Parse GROUP BY clause - support multiple columns
        std::regex groupByRegex("GROUP BY \\{([\\d,\\s]+)\\}");
        std::smatch groupByMatches;
        if (std::regex_search(query, groupByMatches, groupByRegex)) {
            std::string groupByCols = groupByMatches[1].str();
            std::stringstream ss(groupByCols);
            std::string col;
            
            while (std::getline(ss, col, ',')) {
                // Trim whitespace
                col.erase(0, col.find_first_not_of(" \t\n\r"));
                col.erase(col.find_last_not_of(" \t\n\r") + 1);
                
                if (!col.empty()) {
                    size_t colIndex = std::stoi(col) - 1;
                    components.groupByAttributes.push_back(colIndex);
                    
                    if (components.groupByAttributes.size() == 1) {
                        components.groupBy = true;
                        components.groupByAttributeIndex = colIndex;
                    }
                }
            }
        }
    
        // Check for WHERE clause - support multiple conditions separated by AND
        size_t where_pos = query.find("WHERE");
        if (where_pos != std::string::npos) {
            // Extract the WHERE clause part (everything after WHERE until GROUP BY, ORDER BY, or end)
            size_t where_end = query.length();
            size_t group_pos = query.find("GROUP BY", where_pos);
            size_t order_pos = query.find("ORDER BY", where_pos);
            
            if (group_pos != std::string::npos) where_end = std::min(where_end, group_pos);
            if (order_pos != std::string::npos) where_end = std::min(where_end, order_pos);
            
            std::string where_clause = query.substr(where_pos + 5, where_end - where_pos - 5); // +5 to skip "WHERE"
            
            // Split by "and" or "AND"
            std::regex and_regex("\\s+(?:and|AND)\\s+");
            std::sregex_token_iterator iter(where_clause.begin(), where_clause.end(), and_regex, -1);
            std::sregex_token_iterator end;
            
            // Parse each individual condition
            std::regex conditionRegex(R"(\{(\d+)\}\s*(==|!=|>=|<=|>|<)\s*([-+]?[0-9]*\.?[0-9]+|"[^"]*"|'[^']*'))");
            
            for (; iter != end; ++iter) {
                std::string condition = iter->str();
                std::smatch conditionMatch;
                
                if (std::regex_search(condition, conditionMatch, conditionRegex)) {
                    size_t attrIndex = std::stoi(conditionMatch[1]) - 1;
                    std::string opStr = conditionMatch[2];
                    std::string valueStr = conditionMatch[3];
                    
                    ComparisonOperator op = parseOperator(opStr);
                    Field value = parseFieldValue(valueStr);                    
                    components.whereConditions.emplace_back(attrIndex, op, std::move(value));
                }
            }
            
            if (!components.whereConditions.empty()) {
                components.whereCondition = true;
            }
        }
    
        // TODO: Add your implementation here for parsing 'JOIN'
        std::regex joinRegex(R"(JOIN\s*\{([^}]+)\}\s*ON\s*\{(\d+)\}\s*=\s*\{(\d+)\})");
        std::smatch joinMatches;
        if (std::regex_search(query, joinMatches, joinRegex)) {
            components.innerJoin = true;
            components.joinRelation = joinMatches[1].str();
            components.joinAttributeIndex1 = std::stoi(joinMatches[2].str()) - 1; // left side index
            components.joinAttributeIndex2 = std::stoi(joinMatches[3].str()) - 1; // right side index
        }

        // TODO: Add your implementation here for parsing 'ORDER BY'
        // Parse ORDER BY {k} (ascending only)
        std::regex orderRegex(R"(ORDER BY\s*\{(\d+)\})");
        std::smatch orderMatches;
        if (std::regex_search(query, orderMatches, orderRegex)) {
            components.orderBy = true;
            components.orderByAttributeIndex = std::stoi(orderMatches[1].str()) - 1; // 0-based
        }
    }

    std::vector<std::vector<std::unique_ptr<Field>>> executeQuery() {
        // Stack allocation of ScanOperator
        ScanOperator scanOp(buffer_manager, components.relation);
        ScanOperator scanOp2(buffer_manager, components.joinRelation);

        // Using a pointer to Operator to handle polymorphism
        Operator* rootOp = &scanOp;

        std::unique_ptr<SelectOperator> selectOpBuffer;
        std::unique_ptr<HashAggregationOperator> hashAggOpBuffer;
        std::unique_ptr<ProjectOperator> projectOpBuffer;
        std::unique_ptr<HashJoin> hashJoinOpBuffer;

        // TODO: Add your implementation here for applying 'JOIN'
        if (components.innerJoin) {
            hashJoinOpBuffer = std::make_unique<HashJoin>(
                scanOp,
                scanOp2,
                static_cast<size_t>(components.joinAttributeIndex1),
                static_cast<size_t>(components.joinAttributeIndex2)
            );
            rootOp = hashJoinOpBuffer.get();
        }
        // Step 1: Apply WHERE conditions
        if (!components.whereConditions.empty()) {
            if (components.whereConditions.size() == 1) {
                // Single condition - use SimplePredicate directly
                const auto& cond = components.whereConditions[0];
                auto predicate = std::make_unique<SimplePredicate>(
                    SimplePredicate::Operand(cond.attributeIndex),
                    SimplePredicate::Operand(std::make_unique<Field>(cond.value)),
                    cond.op
                );
                selectOpBuffer = std::make_unique<SelectOperator>(*rootOp, std::move(predicate));
            } else {
                // Multiple conditions - combine with AND using ComplexPredicate
                auto complexPredicate = std::make_unique<ComplexPredicate>(
                    ComplexPredicate::LogicOperator::AND
                );
                
                for (const auto& cond : components.whereConditions) {
                    auto predicate = std::make_unique<SimplePredicate>(
                        SimplePredicate::Operand(cond.attributeIndex),
                        SimplePredicate::Operand(std::make_unique<Field>(cond.value)),
                        cond.op
                    );
                    complexPredicate->addPredicate(std::move(predicate));
                }
                selectOpBuffer = std::make_unique<SelectOperator>(*rootOp, std::move(complexPredicate));
            }
            rootOp = selectOpBuffer.get();
        }

        // Step 2: Apply GROUP BY and Aggregation
        // With aggregation, we pass SELECT attributes so it can project from first tuple + append aggregates
        // Note: GROUP BY and aggregate functions use column indices after join
        // TODO: Add your implementation here for applying 'GROUP BY' and 'Aggregation'
        bool hasAggregation =
            !components.aggregateFunctions.empty() ||
            !components.groupByAttributes.empty();

        if (hasAggregation) {
            hashAggOpBuffer = std::make_unique<HashAggregationOperator>(
                *rootOp,
                components.groupByAttributes,
                components.aggregateFunctions,
                components.selectAttributes   // non-aggregated SELECT columns
            );
            rootOp = hashAggOpBuffer.get();
        }
        

        // Step 3: Apply Projection (SELECT clause)
        // IMPORTANT: Skip projection if we have aggregation, as GROUP BY already determines output schema
        // After this point, column indices change!
        std::unique_ptr<Sort> sortOpBuffer;
        int projectedOrderByIndex = -1;
         bool hasAggregation_forProjection =
            !components.aggregateFunctions.empty() ||
            !components.groupByAttributes.empty();

        if (!components.selectAttributes.empty() && !hasAggregation_forProjection) {
            // If we have ORDER BY, find the new index after projection
            if (components.orderBy) {
                projectedOrderByIndex = -1;
                for (size_t i = 0; i < components.selectAttributes.size(); ++i) {
                    if (components.selectAttributes[i] == static_cast<size_t>(components.orderByAttributeIndex)) {
                        projectedOrderByIndex = static_cast<int>(i);
                        break;
                    }
                }

                if (projectedOrderByIndex == -1) {
                    // ORDER BY column not in SELECT list: append it to projection
                    std::vector<size_t> extendedAttrs = components.selectAttributes;
                    extendedAttrs.push_back(components.orderByAttributeIndex);
                    projectedOrderByIndex = static_cast<int>(extendedAttrs.size() - 1);
                    projectOpBuffer = std::make_unique<ProjectOperator>(*rootOp, extendedAttrs);
                } else {
                    projectOpBuffer = std::make_unique<ProjectOperator>(*rootOp, components.selectAttributes);
                }
            } else {
                projectOpBuffer = std::make_unique<ProjectOperator>(*rootOp, components.selectAttributes);
            }
            rootOp = projectOpBuffer.get();
        } else if (components.orderBy && !hasAggregation) {
            // No projection, ORDER BY uses original indices
            projectedOrderByIndex = components.orderByAttributeIndex;
        }

        // Step 4: Apply ORDER BY (after projection, using new indices)
        // TODO: Add your implementation here for applying 'ORDER BY'
        if (components.orderBy) {
            std::vector<Sort::Criterion> criteria;

            size_t orderIndex;
            if (!components.aggregateFunctions.empty() || !components.groupByAttributes.empty()) {
                // With aggregation, ORDER BY indices refer to the aggregated output
                orderIndex = static_cast<size_t>(components.orderByAttributeIndex);
            } else {
                // Without aggregation, ORDER BY refers to the current schema
                orderIndex = static_cast<size_t>(projectedOrderByIndex);
            }

            criteria.push_back(Sort::Criterion{orderIndex, false}); // ascending

            sortOpBuffer = std::make_unique<Sort>(*rootOp, criteria);
            rootOp = sortOpBuffer.get();
        }
        // Execute the Root Operator
        std::vector<std::vector<std::unique_ptr<Field>> > result;
        rootOp->open();
        while (rootOp->next()) {
            // Retrieve and print the current tuple
            const auto& output = rootOp->getOutput();
            std::vector<std::unique_ptr<Field>> tuple;
            for (const auto& field : output) {
                tuple.push_back(std::make_unique<Field>(*field));
            }
            result.push_back(std::move(tuple));
        }
        rootOp->close();
        
        return result;
    }
};


// helper functions for tests
enum class TestRelation {
    STUDENTS,
    GRADES,
    FEEDBACK,
};


const std::vector<std::tuple<int, std::string, int>> relation_students{
    {24002, "Xenokrates", 24},
    {26120, "Fichte", 26},
    {29555, "Feuerbach", 29},
    {28000, "Schopenhauer", 46},
    {24123, "Platon", 50},
    {25198, "Aristoteles", 50},
};

const std::vector<std::tuple<int, int, int>> relation_grades{
    {24002, 5001, 1},
    {24002, 5041, 2},
    {29555, 4630, 2},
    {26120, 5001, 3},
    {24123, 5001, 2},
    {25198, 4630, 1},
    {29555, 5041, 1}
};

const std::vector<std::tuple<int, std::string>> relation_feedback{
    {5001, "good"},
    {5041, "bad"},
    {4630, "average"},
    {5041, "good"},
    {4630, "average"},
    {4630, "good"}
};


std::string sutdentsRelationToString() {
    std::stringstream result;
    for(const auto& student : relation_students) {
        result << std::get<0>(student) << ", " << std::get<1>(student)
            << ", " << std::get<2>(student) << std::endl;
    }
    return result.str();
}


class TestTupleSource : public Operator {
    private:
        std::vector<std::vector<std::unique_ptr<Field>>> tuples;
        size_t current_tuple_index = 0;
        TestRelation type;

    public:
        bool opened = false;
        bool closed = false;

        TestTupleSource(TestRelation type) : type(type) {}

        void open() override {
            current_tuple_index = 0;
            opened = true;

            switch (type) {
                case TestRelation::STUDENTS:
                    for (const auto& student : relation_students) {
                        std::vector<std::unique_ptr<Field>> fields;
                        fields.push_back(std::make_unique<Field>(std::get<0>(student)));
                        fields.push_back(std::make_unique<Field>(std::get<1>(student)));
                        fields.push_back(std::make_unique<Field>(std::get<2>(student)));
                        tuples.push_back(std::move(fields));
                    }
                    break;
                case TestRelation::GRADES:
                    for (const auto& grade : relation_grades) {
                        std::vector<std::unique_ptr<Field>> fields;
                        fields.push_back(std::make_unique<Field>(std::get<0>(grade)));
                        fields.push_back(std::make_unique<Field>(std::get<1>(grade)));
                        fields.push_back(std::make_unique<Field>(std::get<2>(grade)));
                        tuples.push_back(std::move(fields));
                    }
                    break;
                case TestRelation::FEEDBACK:
                    for (const auto& feedback : relation_feedback) {
                        std::vector<std::unique_ptr<Field>> fields;
                        fields.push_back(std::make_unique<Field>(std::get<0>(feedback)));
                        fields.push_back(std::make_unique<Field>(std::get<1>(feedback)));
                        tuples.push_back(std::move(fields));
                    }
                    break;
            }
        }

        bool next() override {
            return current_tuple_index < tuples.size();
        }

        void close() override {
            tuples.clear();
            current_tuple_index = 0;
            closed = true;
            opened = false;
        }

        std::vector<std::unique_ptr<Field>> getOutput() override {
            if (current_tuple_index < tuples.size()) {
                std::vector<std::unique_ptr<Field>> result;
                for (const auto& field : tuples[current_tuple_index]) {
                    result.push_back(std::unique_ptr<Field>(field->clone()));
                }
                current_tuple_index++;
                return result;
            }
            return {};
        }
};


std::string sort_output(const std::string& str) {
    std::vector<std::string> lines;
    size_t str_pos = 0;
    while (true) {
        size_t new_str_pos = str.find('\n', str_pos);
        if (new_str_pos == std::string::npos) {
            lines.emplace_back(&str[str_pos], str.size() - str_pos);
            break;
        }
        lines.emplace_back(&str[str_pos], new_str_pos - str_pos + 1);
        str_pos = new_str_pos + 1;
        if (str_pos == str.size()) {
            break;
        }
    }
    std::sort(lines.begin(), lines.end());
    std::string sorted_str;
    for (auto& line : lines) {
        sorted_str.append(line);
    }
    return sorted_str;
}


void write_testcase_csv (
    const std::vector<std::vector<std::unique_ptr<Field>>>& result,
    int tc_number,
    const std::string& out_dir = "outputs"
) {
    // Ensure output directory exists
    std::filesystem::create_directories(out_dir);

    // Build file path: tc<tc_number>.csv
    std::string filename = out_dir + "/tc" + std::to_string(tc_number) + ".csv";

    std::ofstream out(filename, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open output file: " + filename);
    }

    for (const auto& row : result) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i) out << ',';
            const std::string val = row[i] ? row[i]->asString() : "";
            out << val;
        }
        out << '\n';
    }

    out.flush();
    if (!out) {
        throw std::runtime_error("I/O error while writing file: " + filename);
    }
}


int main(int argc, char* argv[]) {  
    bool execute_all = false;
    std::string selected_test = "-1";

    if(argc < 2) {
        execute_all = true;
    } else {
        selected_test = argv[1];
    }

    auto run_test = [](int tc_number, const std::vector<std::vector<std::unique_ptr<Field>>>& result) {
        std::ifstream in("outputs/tc" + std::to_string(tc_number) + ".csv");
        if (!in) {
            throw std::runtime_error("Could not open test case file: outputs/tc" + std::to_string(tc_number) + ".csv");
        }

        std::string expected;
        size_t idx = 0;
        while (std::getline(in, expected)) {
            ASSERT_WITH_MESSAGE(idx < result.size(), "Expected CSV has more lines than result");
            std::string actual = "";
            auto& row = result[idx];
            for (size_t i = 0; i < row.size(); ++i) {
                if (i) actual.push_back(',');
                actual += row[i] ? row[i]->asString() : "";
            }
            ASSERT_WITH_MESSAGE(trim_inplace(expected) == trim_inplace(actual), "Line " + std::to_string(idx + 1) + " mismatch between expected CSV and result");
            ++idx;
        }
    };

    if(execute_all || selected_test == "1") {
        std::cout << "Executing Test 1 :: Field Comparision" << std::endl;
        Field field_i1(12345);
        Field field_i2(67890);
        Field field_i3(12345);

        Field field_s1("this is a string");
        Field field_s2("yet another stri");
        Field field_s3("this is a string");

        ASSERT_WITH_MESSAGE(field_i1 == field_i3, "Equal Field comparison failed");
        ASSERT_WITH_MESSAGE(field_i1 < field_i2, "Less-than Field comparison failed");
        ASSERT_WITH_MESSAGE(field_i1 != field_i2, "Not-equal Field comparison failed");

        ASSERT_WITH_MESSAGE(field_s1 == field_s3, "Equal Field comparison failed");
        ASSERT_WITH_MESSAGE(field_s1 < field_s2, "Less-than Field comparison failed");
        ASSERT_WITH_MESSAGE(field_s1 != field_s2, "Not-equal Field comparison failed");

        std::cout << "\033[1m\033[32mPassed: Test 1\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "2") {
        std::cout << "Executing Test 2 :: Print" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source(TestRelation::STUDENTS);
        std::stringstream output_stream;
        PrintOperator print_operator(source, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source.opened, "Source open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source.closed, "Source close failed");
        ASSERT_WITH_MESSAGE(output_stream.str() == sutdentsRelationToString(), "PrintOperator output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 2\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "3") {
        std::cout << "Executing Test 3 :: Projection" << std::endl;
        BufferManager buffer_manager;
        std::vector<size_t> attributeIndices = {1,2};

        TestTupleSource source(TestRelation::STUDENTS);
        std::stringstream output_stream;

        ProjectOperator projection(source, attributeIndices);
        PrintOperator print_operator(projection, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source.opened, "Source open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source.closed, "Source close failed");
        auto expected_output = 
            ("Xenokrates, 24\n"
            "Fichte, 26\n"
            "Feuerbach, 29\n"
            "Schopenhauer, 46\n"
            "Platon, 50\n"
            "Aristoteles, 50\n"s);

        ASSERT_WITH_MESSAGE(output_stream.str() == expected_output, "ProjectOperator output does not match. Failed");
        
        std::cout << "\033[1m\033[32mPassed: Test 3\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "4") {
        std::cout << "Executing Test 4 :: Sort" << std::endl;
        BufferManager buffer_manager;
        std::vector<Sort::Criterion> criteria = { {1, true}, {2, false} };

        TestTupleSource source(TestRelation::GRADES);
        std::stringstream output_stream;

        Sort sort_operator(source, criteria);
        PrintOperator print_operator(sort_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source.opened, "Source open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source.closed, "Source close failed");
        auto expected_output = 
            ("29555, 5041, 1\n"
            "24002, 5041, 2\n"
            "24002, 5001, 1\n"
            "24123, 5001, 2\n"
            "26120, 5001, 3\n"
            "25198, 4630, 1\n"
            "29555, 4630, 2\n"s);

        ASSERT_WITH_MESSAGE(output_stream.str() == expected_output, "SortOperator output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 4\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "5") {
        std::cout << "Executing Test 5 :: HashJoin" << std::endl;
        
        TestTupleSource source_students(TestRelation::STUDENTS);
        TestTupleSource source_grades(TestRelation::GRADES);
        BufferManager buffer_manager;

        HashJoin join_operator(source_students, source_grades, 0, 0);
        std::stringstream output_stream;
        PrintOperator print_operator(join_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002, Xenokrates, 24, 24002, 5001, 1\n"
             "24002, Xenokrates, 24, 24002, 5041, 2\n"
             "24123, Platon, 50, 24123, 5001, 2\n"
             "25198, Aristoteles, 50, 25198, 4630, 1\n"
             "26120, Fichte, 26, 26120, 5001, 3\n"
             "29555, Feuerbach, 29, 29555, 4630, 2\n"
             "29555, Feuerbach, 29, 29555, 5041, 1\n"s);

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "HashJoin output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 5\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "6"){
        std::cout << "Executing Test 6 :: Union" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(TestRelation::STUDENTS);
        TestTupleSource source_grades(TestRelation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        UnionOperator union_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(union_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "24123\n"
             "25198\n"
             "26120\n"
             "28000\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "UnionOperator output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 6\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "7"){
        std::cout << "Executing Test 7 :: UnionAll" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(TestRelation::STUDENTS);
        TestTupleSource source_grades(TestRelation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        UnionAll union_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(union_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "24002\n"
             "24002\n"
             "24123\n"
             "24123\n"
             "25198\n"
             "25198\n"
             "26120\n"
             "26120\n"
             "28000\n"
             "29555\n"
             "29555\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "UnionAll output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 7\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "8") {
        std::cout << "Executing Test 8 :: Intersect" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(TestRelation::STUDENTS);
        TestTupleSource source_grades(TestRelation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        Intersect intersect_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(intersect_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "24123\n"
             "25198\n"
             "26120\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "Intersect output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 8\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "9") {
        std::cout << "Executing Test 9 :: IntersectAll" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_grades(TestRelation::GRADES);
        TestTupleSource source_feedback(TestRelation::FEEDBACK);

        ProjectOperator project_grades(source_grades, {1});
        ProjectOperator project_feedback(source_feedback, {0});

        IntersectAll intersect_operator(project_grades, project_feedback);
        std::stringstream output_stream;
        PrintOperator print_operator(intersect_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_feedback.opened, "Source feedback open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_feedback.closed, "Source feedback close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("4630\n"
             "4630\n"
             "5001\n"
             "5041\n"
             "5041\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "IntersectAll output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 9\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "10"){
        std::cout << "Executing Test 10 :: Except" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(TestRelation::STUDENTS);
        TestTupleSource source_grades(TestRelation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        Except except_operator(project_students, project_grades);
        std::stringstream output_stream;
        PrintOperator print_operator(except_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("28000\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "Except output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 10\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "11"){
        std::cout << "Executing Test 11 :: ExceptAll" << std::endl;
        BufferManager buffer_manager;

        TestTupleSource source_students(TestRelation::STUDENTS);
        TestTupleSource source_grades(TestRelation::GRADES);

        ProjectOperator project_students(source_students, {0});
        ProjectOperator project_grades(source_grades, {0});

        ExceptAll except_operator(project_grades, project_students);
        std::stringstream output_stream;
        PrintOperator print_operator(except_operator, output_stream);

        print_operator.open();
        ASSERT_WITH_MESSAGE(source_students.opened, "Source students open failed");
        ASSERT_WITH_MESSAGE(source_grades.opened, "Source grades open failed");
        while(print_operator.next()) {
        }
        print_operator.close();
        ASSERT_WITH_MESSAGE(source_students.closed, "Source students close failed");
        ASSERT_WITH_MESSAGE(source_grades.closed, "Source grades close failed");

        auto expected_output = 
            ("24002\n"
             "29555\n");

        ASSERT_WITH_MESSAGE(sort_output(output_stream.str()) == expected_output, "ExceptAll output does not match. Failed");

        std::cout << "\033[1m\033[32mPassed: Test 11\033[0m" << std::endl;
    }

    if(execute_all || selected_test == "12") {
        std::cout<<"Executing Test 12 :: Load and Query"<<std::endl;
        
        RelationManager rel_manager;
        bool loaded = rel_manager.loadCSV("users.csv", "users");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load users.csv");
        bool populated = rel_manager.populateDatabase("users");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate users");

        std::string query = "SELECT {*} FROM {users}";
        rel_manager.parseQuery(query);
        auto result = rel_manager.executeQuery();

        run_test(12, result);

        std::cout<<"\033[1m\033[32mPassed: Test 12\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "13") {
        std::cout<<"Executing Test 13 :: Table Join"<<std::endl;
        
        RelationManager rel_manager;
        bool loaded = rel_manager.loadCSV("users.csv", "users");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load users.csv");
        bool populated = rel_manager.populateDatabase("users");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate users");
        loaded = rel_manager.loadCSV("posts.csv", "posts");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load posts.csv");
        populated = rel_manager.populateDatabase("posts");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate posts");

        std::string query = "SELECT {1,2,3,5} FROM {users} JOIN {posts} ON {2} = {3} ORDER BY {5}";
        rel_manager.parseQuery(query);
        auto result = rel_manager.executeQuery();
        
        run_test(13, result);
       
        std::cout<<"\033[1m\033[32mPassed: Test 13\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "14") {
        std::cout<<"Executing Test 14 :: GetAllCommentsFromUser"<<std::endl;
        
        RelationManager rel_manager;
        bool loaded = rel_manager.loadCSV("users.csv", "users");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load users.csv");
        bool populated = rel_manager.populateDatabase("users");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate users");
        loaded = rel_manager.loadCSV("engagements.csv", "engagements");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load engagements.csv");
        populated = rel_manager.populateDatabase("engagements");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate engagements");

        std::string query = "SELECT {2,4,7,8} FROM {users} JOIN {engagements} ON {2} = {3} WHERE {2} == 'ericrobinson' AND {7} == 'comment' ORDER BY {4}";
        rel_manager.parseQuery(query);
        auto result = rel_manager.executeQuery();

        run_test(14, result);

        std::cout<<"\033[1m\033[32mPassed: Test 14\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "15") {
        std::cout<<"Executing Test 15 :: GetEngagementsCountByLocation"<<std::endl;
        
        RelationManager rel_manager;
        bool loaded = rel_manager.loadCSV("users.csv", "users");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load users.csv");
        bool populated = rel_manager.populateDatabase("users");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate users");
        loaded = rel_manager.loadCSV("engagements.csv", "engagements");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load engagements.csv");
        populated = rel_manager.populateDatabase("engagements");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate engagements");

        std::string query = "SELECT {3} FROM {users} JOIN {engagements} ON {2} = {3} WHERE {3} == 'Port Lisa' COUNT {1}";
        rel_manager.parseQuery(query);
        auto result = rel_manager.executeQuery();

        run_test(15, result);

        std::cout<<"\033[1m\033[32mPassed: Test 15\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "16") {
        std::cout<<"Executing Test 16 :: GetLastUserLike"<<std::endl;

        RelationManager rel_manager;
        rel_manager.loadCSV("engagements.csv", "engagements");
        rel_manager.populateDatabase("engagements");

        std::string query = "SELECT {3,6} FROM {engagements} WHERE {4} == 'like' GROUP BY {3} MAX {6} ORDER BY {1}";
        rel_manager.parseQuery(query);
        auto result = rel_manager.executeQuery();
        
        run_test(16, result);

        std::cout<<"\033[1m\033[32mPassed: Test 16\033[0m"<<std::endl;
    }

    if(execute_all || selected_test == "17") {
        std::cout<<"Executing Test 17 :: GetTotalUserViews"<<std::endl;

        RelationManager rel_manager;
        bool loaded = rel_manager.loadCSV("posts.csv", "posts");
        ASSERT_WITH_MESSAGE(loaded, "Failed to load posts.csv");
        bool populated = rel_manager.populateDatabase("posts");
        ASSERT_WITH_MESSAGE(populated, "Failed to populate posts");

        std::string query = "SELECT {3,4} FROM {posts} GROUP BY {3} SUM {4} ORDER BY {1}";
        rel_manager.parseQuery(query);
        auto result = rel_manager.executeQuery();
        
        run_test(17, result);

        std::cout<<"\033[1m\033[32mPassed: Test 17\033[0m"<<std::endl;
    }
}