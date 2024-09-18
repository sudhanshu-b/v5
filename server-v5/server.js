const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const app = express();
const multer = require('multer');  // Import multer for handling file uploads

//Necessary Middlewares
app.use(cors());
app.use(express.json({ limit: '200mb' }));
app.use(express.urlencoded({ limit: '200mb', extended: true }));

//MongoDB Connection and Schema defination
mongoose.connect('mongodb://localhost:27017/Labelling-pro');

const licenseSchema = new mongoose.Schema({
    name: String,
    id: Number,
    url: String
});

const infoSchema = new mongoose.Schema({
    contributor: String,
    date_created: String,
    description: String,
    url: String,
    version: String,
    year: String,
});

const categorySchema = new mongoose.Schema({
    id: Number,
    name: String,
    supercategory: String
});

const imageSchema = new mongoose.Schema({
    id: Number,
    file_name: String,
    flickrurl: String
});

const annotationSchema = new mongoose.Schema({
    id: Number,
    image_id: Number,
    category_id: Number,
    segmentation: Array,
    area: Number,
    bbox: [Number],
    attributes: Object
});

const License = mongoose.model('License', licenseSchema);
const Info = mongoose.model('Info', infoSchema);
const Category = mongoose.model('Category', categorySchema);
const Image = mongoose.model('Image', imageSchema);
const Annotation = mongoose.model('Annotation', annotationSchema);

const upload = multer({
    limits: { fileSize: 200 * 1024 * 1024 }
});

//Dashboard Page
app.get('/data/annotations/category-count', async (req, res) => {
    try {
        // Aggregate to count annotations by category
        const categoryCounts = await Annotation.aggregate([
            {
                $group: {
                    _id: '$category_id',
                    count: { $sum: 1 }
                }
            }
        ]);

        // Retrieve all categories
        const categories = await Category.find({});

        // Map category IDs to names and counts
        const categoryMap = categories.reduce((map, category) => {
            map[category.id] = { name: category.name, count: 0 };
            return map;
        }, {});

        // Update counts in the map
        categoryCounts.forEach(({ _id, count }) => {
            if (categoryMap[_id]) {
                categoryMap[_id].count = count;
            }
        });

        // Convert the map to an array
        const result = Object.keys(categoryMap).map(id => ({
            category_id: parseInt(id),
            name: categoryMap[id].name,
            count: categoryMap[id].count
        }));

        res.json(result);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

//Download JSON Page

app.post('/data/annotations/categories', async (req, res) => {
    try {
        const categoryIds = req.body.category_ids.map(id => parseInt(id));

        // Fetch annotations filtered by category IDs
        const filteredAnnotations = await Annotation.find({ category_id: { $in: categoryIds } });

        // Fetch all categories to map IDs to names
        const categories = await Category.find({});
        const categoriesMap = categories.reduce((acc, category) => {
            acc[category.id] = category.name;
            return acc;
        }, {});

        // Fetch all images to map IDs to file names
        const images = await Image.find({});
        const imagesMap = images.reduce((acc, image) => {
            acc[image.id] = image.file_name;
            return acc;
        }, {});

        // Enrich annotations with category names and image file names
        const enrichedAnnotations = filteredAnnotations.map(annotation => ({
            ...annotation.toObject(), // Convert Mongoose document to plain object
            name: categoriesMap[annotation.category_id],
            file_name: imagesMap[annotation.image_id]
        }));

        res.json(enrichedAnnotations);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

//Table Page Requests
app.get('/data', async (req, res) => {
    try {
        const offset = parseInt(req.query.offset, 10) || 0; 
        const limit = parseInt(req.query.limit, 10) || 10;
        const categoryFilter = req.query.category || ''; // Category filter from query params

        // Define the query object
        let annotationQuery = {};

        // If a category filter is provided, adjust the query to filter annotations by category_id
        if (categoryFilter) {
            const category = await Category.findOne({ _id: categoryFilter }).lean();
            if (category) {
                annotationQuery.category_id = category.id; // Use the `id` field from the category document
            } else {
                // If no category matches the filter, return empty results
                return res.json({ data: [], totalAnnotations: 0 });
            }
        }

        // Fetch annotations with pagination and filtering
        const annotations = await Annotation.find(annotationQuery)
            .skip(offset)
            .limit(limit)
            .lean();

        // Extract related image_ids and category_ids from annotations
        const imageIds = annotations.map(a => a.image_id);
        const categoryIds = annotations.map(a => a.category_id);

        // Fetch related images and categories
        const images = await Image.find({ id: { $in: imageIds } }).lean();
        const categories = await Category.find({ id: { $in: categoryIds } }).lean();

        // Map image_id and category_id to their corresponding objects
        const imageMap = images.reduce((acc, image) => {
            acc[image.id] = image;
            return acc;
        }, {});

        const categoryMap = categories.reduce((acc, category) => {
            acc[category.id] = category;
            return acc;
        }, {});

        // Enrich annotations with image and category details
        const enrichedAnnotations = annotations.map(annotation => ({
            ...annotation,
            file_name: imageMap[annotation.image_id]?.file_name || 'N/A',
            name: categoryMap[annotation.category_id]?.name || 'N/A',
            supercategory: categoryMap[annotation.category_id]?.supercategory || 'N/A'
        }));

        // Get total annotation count for pagination
        const totalAnnotations = await Annotation.countDocuments(annotationQuery);

        // Return enriched annotations with pagination data
        res.json({ data: enrichedAnnotations, totalAnnotations });
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

app.get('/data/categories' ,async(req,res)=>{
    const categories = await Category.find({});
    res.json(categories)
})

//Upload Json Page
app.get('/data/check', async (req, res) => {
    try {
        const categoryCount = await Category.countDocuments();
        const imageCount = await Image.countDocuments();
        const annotationCount = await Annotation.countDocuments();
        res.json({ categories: categoryCount > 0, images: imageCount > 0, annotations: annotationCount > 0 });
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

const getLastIds = async () => {
    try {
        // Get the last inserted image and annotation documents
        const lastImage = await Image.findOne().sort({ id: -1 }).exec();
        const lastAnnotation = await Annotation.findOne().sort({ id: -1 }).exec();

        // Extract the last IDs, or return 0 if no data exists
        const lastImageId = lastImage ? lastImage.id : 0;
        const lastAnnotationId = lastAnnotation ? lastAnnotation.id : 0;

        return { lastImageId, lastAnnotationId };
    } catch (error) {
        console.error('Error fetching last IDs:', error);
        throw error;
    }
};

app.post('/data/update', upload.single('jsonFile'), async (req, res) => {
    try {
        const file = req.file;
        if (!file) {
            return res.status(400).json({ message: 'No file uploaded' });
        }

        const fileContent = file.buffer.toString();
        const jsonData = JSON.parse(fileContent);

        // Check if there's any existing data in the collections
        const categoriesExist = await Category.countDocuments() > 0;
        const imagesExist = await Image.countDocuments() > 0;
        const annotationsExist = await Annotation.countDocuments() > 0;

        if (!categoriesExist || !imagesExist || !annotationsExist) {
            // If any collection is empty, add all data from the JSON file
            const newCategories = jsonData.categories.map(cat => new Category(cat));
            const newImages = jsonData.images.map(img => new Image(img));
            const newAnnotations = jsonData.annotations.map(ann => new Annotation(ann));

            await Category.insertMany(newCategories);
            await Image.insertMany(newImages);
            await Annotation.insertMany(newAnnotations);

            return res.json({ message: 'Data added successfully as collections were empty.' });
        }

        // Fetch last IDs from the database
        const lastIds = await getLastIds();
        const lastImageId = lastIds.lastImageId;
        const lastAnnotationId = lastIds.lastAnnotationId;
        const imageIdMapping = {};

        // Increment image IDs
        const updatedImages = jsonData.images.map((image, index) => {
            const newImageId = lastImageId + index + 1;
            imageIdMapping[image.id] = newImageId; // Map old image id to new image id
            return { ...image, id: newImageId };  // Update image with new id
        });

        // Increment annotation IDs and update image_id in annotations
        const updatedAnnotations = jsonData.annotations.map((annotation, index) => {
            const newAnnotationId = lastAnnotationId + index + 1; // Increment annotation ID
            return {
                ...annotation,
                id: newAnnotationId,  // Update annotation id
                image_id: imageIdMapping[annotation.image_id]  // Map to new image_id
            };
        });

        console.log(`Prepared ${updatedImages.length} images and ${updatedAnnotations.length} annotations for update`);

        // Insert batches into the database
        await updateInBatches(updatedImages, Image);
        await updateInBatches(updatedAnnotations, Annotation);

        res.json({ message: 'Data successfully added with document size checks in place.' });
    } catch (error) {
        console.error("Error during data update:", error);
        res.status(500).json({ message: error.message });
    }
});

// Get last IDs for images and annotations
app.get('/data/last-ids', async (req, res) => {
    try {
        const lastImage = await Image.findOne().sort({ id: -1 }).exec();
        const lastAnnotation = await Annotation.findOne().sort({ id: -1 }).exec();

        const lastImageId = lastImage ? lastImage.id : 0;
        const lastAnnotationId = lastAnnotation ? lastAnnotation.id : 0;

        res.json({ lastImageId, lastAnnotationId });
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

// Utility function to insert in batches
const updateInBatches = async (data, model) => {
    const batchSize = 500;
    const totalBatches = Math.ceil(data.length / batchSize);

    for (let i = 0; i < totalBatches; i++) {
        const batchData = data.slice(i * batchSize, (i + 1) * batchSize);

        try {
            // Use insertMany to insert the batch of documents into the collection
            await model.insertMany(batchData);
            console.log(`Batch ${i + 1}/${totalBatches} inserted successfully`);
        } catch (err) {
            console.error(`Error inserting batch ${i + 1}:`, err);
        }
    }
};

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server started on port ${PORT}`));