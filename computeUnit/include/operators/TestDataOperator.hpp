#pragma once

#include <Utility.hpp>
#include <infrastructure/DataCatalog.hpp>
#include <operators/AbstractOperator.hpp>

/**
 * @brief The operator converts all elements of a column into a string representation.
 *
 */
class TestDataOperator : public AbstractOperator {
   public:
    TestDataOperator(WorkRequest p_request) : AbstractOperator(p_request){};
    virtual WorkResponse run() override;

    /**
     * @brief A dummy method to create three test data sets concurrently, i.e. with parallel threads. Blocks until all data is created.
     *
     * @param test_element_count The amount of elements in each column-
     * @param seed The seed for the random generator.
     */
    static void generate_generic_test_data(const std::string locality_suffix, const size_t test_element_count, const size_t seed, bool verbose) {
        std::thread t1([&]() {
            ddd::testing::TestDataSetup tds;
            tds.set_tablename("test");
            tds.set_columnname("test_uint32" + locality_suffix);
            tds.set_datatype(static_cast<ddd::testing::DataType>(DataType::uint32));
            tds.set_elementcount(test_element_count);
            tds.set_randomseed(seed);

            dispatch_create_test_column(tds);
        });

        std::thread t2([&]() {
            ddd::testing::TestDataSetup tds;
            tds.set_tablename("test");
            tds.set_columnname("test_uint64" + locality_suffix);
            tds.set_datatype(static_cast<ddd::testing::DataType>(DataType::uint64));
            tds.set_elementcount(test_element_count);
            tds.set_randomseed(seed);

            dispatch_create_test_column(tds);
        });

        std::thread t3([&]() {
            ddd::testing::TestDataSetup tds;
            tds.set_tablename("test");
            tds.set_columnname("test_double" + locality_suffix);
            tds.set_datatype(static_cast<ddd::testing::DataType>(DataType::f64));
            tds.set_elementcount(test_element_count);
            tds.set_randomseed(seed);

            dispatch_create_test_column(tds);
        });

        t1.join();
        t2.join();
        t3.join();
    }

    /**
     * @brief Create a test column object. Triggered via TestDataSetup protobuf message.
     *
     * @tparam T The underlying data type.
     * @param tds The protobuf object, which triggers this function.
     * @return true Data was generated and added to the DataCatalog.
     * @return false Data was not generated and nothing was added, i.e. a column with the same ident already exists.
     */
    template <typename T>
    static bool create_test_column(const ddd::testing::TestDataSetup& tds) {
        if (DataCatalog::getInstance().getColumnByName(tds.tablename(), tds.columnname())) {
            return false;
        }

        std::shared_ptr<Column> testcol = std::make_shared<Column>(0, static_cast<DataType>(tds.datatype()), tds.elementcount() * sizeof(T), tds.elementcount(), false, true);
        testcol->allocate();
        tuddbs::Utility::generateRandomColumn<T>(static_cast<T*>(testcol->data), tds.elementcount(), tds.randomseed());

        for (size_t i = 0; i < 10; i++) {
            std::cout << static_cast<T*>(testcol->data)[i] << " ";
        }
        std::cout << std::endl;
        DataCatalog::getInstance().addColumn(tds.tablename(), tds.columnname(), testcol);
        return true;
    }

    /**
     * @brief Select the data type for the data generation, based on a given protobuf message.
     *
     * @param tds The protobuf message, which contains all generation information.
     * @return std::pair<std::string, bool> Stringified WorkResponse information and success indicator.
     */
    static std::pair<std::string, bool> dispatch_create_test_column(ddd::testing::TestDataSetup tds) {
        std::string info = "[TestDataSetup] Column " + tds.tablename() + "." + tds.columnname() + " succesfully created.";
        bool success = false;
        bool datatype_unknown = false;
        switch (static_cast<DataType>(tds.datatype())) {
            case DataType::int8: {
                success = create_test_column<int8_t>(tds);
            } break;
            case DataType::uint8: {
                success = create_test_column<uint8_t>(tds);
            } break;
            case DataType::int16: {
                success = create_test_column<int16_t>(tds);
            } break;
            case DataType::uint16: {
                success = create_test_column<uint16_t>(tds);
            } break;
            case DataType::int32: {
                success = create_test_column<int32_t>(tds);
            } break;
            case DataType::uint32: {
                success = create_test_column<uint32_t>(tds);
            } break;
            case DataType::int64: {
                success = create_test_column<int64_t>(tds);
            } break;
            case DataType::uint64: {
                success = create_test_column<uint64_t>(tds);
            } break;
            case DataType::f32: {
                success = create_test_column<float>(tds);
            } break;
            case DataType::f64: {
                success = create_test_column<double>(tds);
            } break;
            default: {
                datatype_unknown = true;
                info = "[TestDataSetup] Error allocating data: Invalid datatype submitted or datatype not supported yet. Nothing was allocated.";
                LOG_ERROR("" << info << std::endl;)
            }
        }

        if (!success && !datatype_unknown) {
            info = "[TestDataSetup] Could not craete test column. Identifier already exists.";
        }

        return {info, success};
    }

    /**
     * @brief Ask the global DataCatalog to remove a column by identifier.
     *
     * @param tds The protobuf message, which triggers deletion.
     * @return std::pair<std::string, bool> Stringified WorkResponse information and success indicator.
     */
    std::pair<std::string, bool> dispatch_remove_column(ddd::testing::TestDataSetup tds) {
        if (DataCatalog::getInstance().removeColumn(tds.tablename(), tds.columnname()) > 0) {
            return {"[TestDataSetup] Successfully removed " + tds.columnname(), true};
        } else {
            return {"[TestDataSetup] Column " + tds.columnname() + " did not exists. Nothing done.", false};
        }
    }
};